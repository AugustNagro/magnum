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

  case class Car(
      model: String,
      @Id id: UUID,
      topSpeed: Int,
      created: OffsetDateTime
  ) derives DbReader

  val carSchema = DbSchema[Car, Car, UUID](
    ClickhouseDbType,
    SqlNameMapper.CamelToSnakeCase
  )

  val carRepo = ImmutableRepo(carSchema)

  val allCars = Vector(
    Car(
      "McLaren Senna",
      UUID.fromString("a88a32f1-1e4a-41b9-9fb0-e9a8aba2428a"),
      208,
      OffsetDateTime.of(2023, 3, 5, 2, 26, 0, 0, ZoneOffset.UTC)
    ),
    Car(
      "Ferrari F8 Tributo",
      UUID.fromString("e4895170-5b54-4e3b-b857-b95d45d3550c"),
      212,
      OffsetDateTime.of(2023, 3, 5, 2, 27, 0, 0, ZoneOffset.UTC)
    ),
    Car(
      "Aston Martin Superleggera",
      UUID.fromString("460798da-917d-442f-a987-a7e6528ddf17"),
      211,
      OffsetDateTime.of(2023, 3, 5, 2, 28, 0, 0, ZoneOffset.UTC)
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
      val spec = Spec(carSchema)
        .where(sql"${carSchema.topSpeed} > 211")
      assertEquals(carRepo.findAll(spec).size, 1)

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

  test("select query"):
    connect(ds()):
      val car = carSchema
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"

      assertNoDiff(
        query.query,
        "select model, id, top_speed, created from car where top_speed > ?"
      )
      assertEquals(query.params, Vector(minSpeed))
      assertEquals(
        query.run[Car],
        allCars.tail
      )

  test("select query with aliasing"):
    connect(ds()):
      val car = carSchema.alias("c")
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"

      assertNoDiff(
        query.query,
        "select c.model, c.id, c.top_speed, c.created from car c where c.top_speed > ?"
      )
      assertEquals(query.run[Car], allCars.tail)

  /*
  Repo Tests
   */

  case class Person(
      id: UUID,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime
  )

  val person = DbSchema[Person, Person, UUID](
    ClickhouseDbType,
    SqlNameMapper.CamelToSnakeCase
  )

  // aliases should not affect generated queries
  val personRepo = Repo(person.alias("p"))

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
      case class UserCreator(name: String)
      case class User(id: UUID, name: String) derives DbReader
      val userSchema = DbSchema[UserCreator, User, UUID](
        ClickhouseDbType
      )

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
