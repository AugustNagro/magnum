import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.dimafeng.testcontainers.{
  ClickHouseContainer,
  ContainerDef,
  JdbcDatabaseContainer
}
import munit.{FunSuite, Location, TestOptions}
import com.clickhouse.jdbc.ClickHouseDataSource
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.{Properties, UUID}
import javax.sql.DataSource
import scala.util.Using

class ClickHouseTests extends FunSuite, TestContainersFixtures:

  /*
  Immutable Repo Tests
   */

  enum Color derives DbCodec:
    case Red, Green, Blue

  @Table(ClickhouseDbType, SqlNameMapper.CamelToSnakeCase)
  case class Car(
      model: String,
      @Id id: UUID,
      topSpeed: Int,
      created: OffsetDateTime,
      @SqlName("vin") vinNumber: Option[Int],
      color: Color
  ) derives DbCodec

  object carRepo extends ImmutableRepo[Car, UUID]:
    def customSelect(speed: Int): Vector[Car] =
      val sql = sql"SELECT ${*} FROM $table WHERE top_speed > $speed"
      connect(ds()):
        sql.query[Car].run()

  val allCars = Vector(
    Car(
      "McLaren Senna",
      UUID.fromString("a88a32f1-1e4a-41b9-9fb0-e9a8aba2428a"),
      208,
      OffsetDateTime.of(2023, 3, 5, 2, 26, 0, 0, ZoneOffset.UTC),
      Some(123),
      Color.Red
    ),
    Car(
      "Ferrari F8 Tributo",
      UUID.fromString("e4895170-5b54-4e3b-b857-b95d45d3550c"),
      212,
      OffsetDateTime.of(2023, 3, 5, 2, 27, 0, 0, ZoneOffset.UTC),
      Some(124),
      Color.Green
    ),
    Car(
      "Aston Martin Superleggera",
      UUID.fromString("460798da-917d-442f-a987-a7e6528ddf17"),
      211,
      OffsetDateTime.of(2023, 3, 5, 2, 28, 0, 0, ZoneOffset.UTC),
      None,
      Color.Blue
    )
  )

  test("count"):
    connect(ds()):
      assertEquals(carRepo.count, 3L)

  test("existsById"):
    connect(ds()):
      assert(carRepo.existsById(allCars.head.id))
      assert(!carRepo.existsById(UUID.randomUUID))

  test("findAll"):
    val cars = connect(ds()):
      carRepo.findAll
    assertEquals(cars, allCars)

  test("findAll spec"):
    connect(ds()):
      val topSpeed = 211
      val spec = Spec[Car]
        .where(sql"top_speed > $topSpeed")
      assertEquals(carRepo.findAll(spec), Vector(allCars(1)))

  test("findById"):
    connect(ds()):
      assertEquals(carRepo.findById(allCars.last.id).get, allCars.last)
      assertEquals(carRepo.findById(UUID.randomUUID), None)

  test("findAllByIds"):
    intercept[UnsupportedOperationException]:
      connect(ds()):
        assertEquals(
          carRepo.findAllById(Vector(allCars(0).id, allCars(1).id)).size,
          2
        )

  test("repeatable read transaction"):
    transact(ds(), withRepeatableRead):
      assertEquals(carRepo.count, 3L)

  private def withRepeatableRead(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)

  test("select query"):
    connect(ds()):
      val minSpeed = 210
      val query =
        sql"select * from car where top_speed > $minSpeed".query[Car]
      assertNoDiff(
        query.frag.sqlString,
        "select * from car where top_speed > ?"
      )
      assertEquals(query.frag.params, Vector(minSpeed))
      assertEquals(query.run(), allCars.tail)

  test("select via option"):
    connect(ds()):
      val vin = Some(124)
      val cars =
        sql"select * from car where vin = $vin"
          .query[Car]
          .run()
      assertEquals(cars, allCars.filter(_.vinNumber == vin))

  test("tuple select"):
    connect(ds()):
      val tuples = sql"select model, color from car where id = ${allCars(1).id}"
        .query[(String, Color)]
        .run()
      assertEquals(tuples, Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    connect(ds()):
      assertEquals(carRepo.findAll.last.vinNumber, None)

  test("custom select using identifiers"):
    connect(ds()):
      assertEquals(carRepo.customSelect(211), Vector(allCars(1)))

  /*
  Repo Tests
   */

  @Table(ClickhouseDbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: UUID,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime
  ) derives DbCodec

  object personRepo extends Repo[Person, Person, UUID]:
    def customInsert(p: Person): Unit =
      val sql =
        sql"INSERT INTO $table $insertColumns VALUES ($p)"
      connect(ds()):
        sql.update.run()

  test("delete"):
    connect(ds()):
      val p = personRepo.findAll.head
      personRepo.delete(p)
      assertEquals(personRepo.findById(p.id), None)

  test("delete invalid"):
    connect(ds()):
      val p = personRepo.findAll.head.copy(id = UUID.randomUUID)
      personRepo.delete(p)
      assertEquals(8L, personRepo.count)

  test("deleteById"):
    connect(ds()):
      val p = personRepo.findAll.head
      personRepo.deleteById(p.id)
      personRepo.deleteById(UUID.randomUUID)
      assertEquals(personRepo.count, 7L)

  test("truncate"):
    connect(ds()):
      personRepo.truncate()
      assertEquals(personRepo.count, 0L)

  test("insert"):
    connect(ds()):
      personRepo.insert(
        Person(
          id = UUID.randomUUID,
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          created = OffsetDateTime.now
        )
      )
      personRepo.insert(
        Person(
          id = UUID.randomUUID,
          firstName = None,
          lastName = "Prince",
          isAdmin = true,
          created = OffsetDateTime.now
        )
      )
      assertEquals(personRepo.count, 10L)
      assert(personRepo.findAll.exists(_.lastName == "Prince"))

  test("insertAll"):
    connect(ds()):
      personRepo.insertAll(
        Vector(
          Person(
            id = UUID.randomUUID,
            firstName = Some("John"),
            lastName = "Smith",
            isAdmin = false,
            created = OffsetDateTime.now
          ),
          Person(
            id = UUID.randomUUID,
            firstName = None,
            lastName = "Prince",
            isAdmin = true,
            created = OffsetDateTime.now
          )
        )
      )
      assertEquals(personRepo.count, 10L)

  test("insertReturning"):
    connect(ds()):
      val id = UUID.randomUUID
      val person = personRepo.insertReturning(
        Person(
          id = id,
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          created = OffsetDateTime.now
        )
      )
      assertEquals(personRepo.count, 9L)
      assertEquals(personRepo.findById(id).get.firstName, person.firstName)

  test("insertAllReturning"):
    connect(ds()):
      val ps = Vector(
        Person(
          id = UUID.randomUUID,
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          created = OffsetDateTime.now
        ),
        Person(
          id = UUID.randomUUID,
          firstName = None,
          lastName = "Prince",
          isAdmin = true,
          created = OffsetDateTime.now
        )
      )
      val people = personRepo.insertAllReturning(ps)
      assertEquals(people, ps)
      assertEquals(personRepo.count, 10L)

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = Person(
          id = UUID.randomUUID,
          firstName = None,
          lastName = null,
          isAdmin = false,
          created = OffsetDateTime.now
        )
        personRepo.insert(invalidP)

  test("only allows EC =:= E"):
    intercept[IllegalArgumentException]:
      case class UserCreator(name: String) derives DbCodec
      @Table(ClickhouseDbType)
      case class User(id: UUID, name: String) derives DbCodec
      val repo = Repo[UserCreator, User, UUID]

  test("update"):
    intercept[UnsupportedOperationException]:
      connect(ds()):
        val p = personRepo.findAll.head
        val updated = p.copy(firstName = None)
        personRepo.update(updated)
        assertEquals(personRepo.findById(p.id).get, updated)

  test("insertAll"):
    connect(ds()):
      val newPeople = Vector(
        Person(
          id = UUID.randomUUID,
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          created = OffsetDateTime.now
        ),
        Person(
          id = UUID.randomUUID,
          firstName = None,
          lastName = "Prince",
          isAdmin = true,
          created = OffsetDateTime.now
        )
      )
      personRepo.insertAll(newPeople)
      assertEquals(personRepo.count, 10L)
      assert(personRepo.findAll.exists(_.lastName == "Smith"))

  test("updateAll"):
    intercept[UnsupportedOperationException]:
      connect(ds()):
        val allPeople = personRepo.findAll
        val newPeople = Vector(
          allPeople(0).copy(lastName = "Peterson"),
          allPeople(1).copy(lastName = "Moreno")
        )
        personRepo.updateAll(newPeople)
        assertEquals(personRepo.findById(allPeople(0).id).get, newPeople(0))
        assertEquals(personRepo.findById(allPeople(1).id).get, newPeople(1))

  test("transact"):
    val count = transact(ds())(personRepo.count)
    assertEquals(count, 8L)

  test("custom insert"):
    val person = Person(
      id = UUID.randomUUID,
      firstName = Some("John"),
      lastName = "Smith",
      isAdmin = false,
      created = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    )

    connect(ds()):
      personRepo.customInsert(person)

      assertEquals(personRepo.count, 9L)
      assertEquals(personRepo.findById(person.id).get, person)

  val clickHouseContainer = ForAllContainerFixture(
    ClickHouseContainer
      .Def(dockerImageName =
        DockerImageName.parse("clickhouse/clickhouse-server:23.2.2.20")
      )
      .createContainer()
  )

  override def munitFixtures: Seq[Fixture[_]] =
    super.munitFixtures :+ clickHouseContainer

  def ds(): DataSource =
    val clickHouse = clickHouseContainer()
    val ds = ClickHouseDataSource(clickHouse.jdbcUrl)
    val carSql = Files.readString(
      Path.of(getClass.getResource("/clickhouse-car.sql").toURI)
    )
    val personSql = Files.readString(
      Path.of(getClass.getResource("/clickhouse-person.sql").toURI)
    )
    Using
      .Manager(use =>
        val con = use(ds.getConnection)
        val stmt = use(con.createStatement)
        stmt.execute(carSql)
        stmt.execute(personSql)
      )
      .get
    ds
