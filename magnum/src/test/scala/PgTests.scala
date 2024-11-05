import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.dimafeng.testcontainers.{
  ContainerDef,
  JdbcDatabaseContainer,
  PostgreSQLContainer
}
import munit.{FunSuite, Location, TestOptions}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.UUID
import javax.sql.DataSource
import scala.util.Using
import scala.util.Using.Manager

class PgTests extends FunSuite, TestContainersFixtures:

  /*
  Immutable Repo Tests
   */

  enum Color derives DbCodec:
    case Red, Green, Blue

  @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  case class Car(
      model: String,
      @Id id: Long,
      topSpeed: Int,
      @SqlName("vin") vinNumber: Option[Int],
      color: Color
  ) derives DbCodec

  val carRepo = ImmutableRepo[Car, Long]
  val car = TableInfo[Car, Car, Long]

  val allCars = Vector(
    Car("McLaren Senna", 1L, 208, Some(123), Color.Red),
    Car("Ferrari F8 Tributo", 2L, 212, Some(124), Color.Green),
    Car("Aston Martin Superleggera", 3L, 211, None, Color.Blue)
  )

  test("count"):
    connect(ds()):
      assertEquals(carRepo.count, 3L)

  test("existsById"):
    connect(ds()):
      assert(carRepo.existsById(3L))
      assert(!carRepo.existsById(4L))

  test("findAll"):
    val cars = connect(ds()):
      carRepo.findAll
    assertEquals(cars, allCars)

  test("findAll spec"):
    connect(ds()):
      val topSpeed = 211
      val spec = Spec[Car]
        .where(sql"${car.topSpeed} > $topSpeed")
      assertEquals(carRepo.findAll(spec), Vector(allCars(1)))

  test("findAll spec with multiple conditions"):
    connect(ds()):
      val topSpeed = 211
      val model = "Ferrari F8 Tributo"
      val spec = Spec[Car]
        .where(sql"${car.topSpeed} > $topSpeed")
        .where(sql"${car.model} = $model")
      assertEquals(carRepo.findAll(spec), Vector(allCars(1)))

  test("findById"):
    connect(ds()):
      assertEquals(carRepo.findById(3L).get, allCars.last)
      assertEquals(carRepo.findById(4L), None)

  test("findAllByIds"):
    connect(ds()):
      assertEquals(
        carRepo.findAllById(Vector(1L, 3L)).map(_.id),
        Vector(1L, 3L)
      )

  test("repeatable read transaction"):
    transact(ds(), withRepeatableRead):
      assertEquals(carRepo.count, 3L)

  private def withRepeatableRead(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)

  test("select query"):
    connect(ds()):
      val minSpeed: Int = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
          .query[Car]
      assertNoDiff(
        query.frag.sqlString,
        "select model, id, top_speed, vin, color from car where top_speed > ?"
      )
      assertEquals(query.frag.params, Vector(minSpeed))
      assertEquals(query.run(), allCars.tail)

  test("select query with aliasing"):
    connect(ds()):
      val minSpeed = 210
      val cAlias = car.alias("c")
      val query =
        sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
          .query[Car]
      assertNoDiff(
        query.frag.sqlString,
        "select c.model, c.id, c.top_speed, c.vin, c.color from car c where c.top_speed > ?"
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
      val tuples = sql"select model, color from car where id = 2"
        .query[(String, Color)]
        .run()
      assertEquals(tuples, Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    connect(ds()):
      assertEquals(carRepo.findById(3L).get.vinNumber, None)

  /*
  Repo Tests
   */
  case class PersonCreator(
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      socialId: Option[UUID]
  ) derives DbCodec

  @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime,
      socialId: Option[UUID]
  ) derives DbCodec

  val personRepo = Repo[PersonCreator, Person, Long]
  val person = TableInfo[PersonCreator, Person, Long]

  test("delete"):
    connect(ds()):
      val p = personRepo.findById(1L).get
      personRepo.delete(p)
      assertEquals(personRepo.findById(1L), None)

  test("delete invalid"):
    connect(ds()):
      personRepo.delete(Person(23L, None, "", false, OffsetDateTime.now, None))
      assertEquals(8L, personRepo.count)

  test("deleteById"):
    connect(ds()):
      personRepo.deleteById(1L)
      personRepo.deleteById(2L)
      personRepo.deleteById(1L)
      assertEquals(personRepo.findAll.size, 6)

  test("deleteAll"):
    connect(ds()):
      val p1 = personRepo.findById(1L).get
      val p2 = p1.copy(id = 2L)
      val p3 = p1.copy(id = 99L)
      assertEquals(
        personRepo.deleteAll(Vector(p1, p2, p3)),
        BatchUpdateResult.Success(2)
      )
      assertEquals(6L, personRepo.count)

  test("deleteAllById"):
    connect(ds()):
      assertEquals(
        personRepo.deleteAllById(Vector(1L, 2L, 1L)),
        BatchUpdateResult.Success(2)
      )
      assertEquals(6L, personRepo.count)

  test("truncate"):
    connect(ds()):
      personRepo.truncate()
      assertEquals(personRepo.count, 0L)

  test("insert"):
    connect(ds()):
      personRepo.insert(
        PersonCreator(
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          socialId = Some(UUID.randomUUID())
        )
      )
      personRepo.insert(
        PersonCreator(
          firstName = None,
          lastName = "Prince",
          isAdmin = true,
          socialId = None
        )
      )
      assertEquals(personRepo.count, 10L)
      assertEquals(personRepo.findById(9L).get.lastName, "Smith")

  test("insertReturning"):
    connect(ds()):
      val person = personRepo.insertReturning(
        PersonCreator(
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          socialId = None
        )
      )
      assertEquals(person.id, 9L)
      assertEquals(person.lastName, "Smith")

  test("insertAllReturning"):
    connect(ds()):
      val newPc = Vector(
        PersonCreator(
          firstName = Some("Chandler"),
          lastName = "Johnsored",
          isAdmin = true,
          socialId = Some(UUID.randomUUID())
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = false,
          socialId = None
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = true,
          socialId = None
        )
      )
      val people = personRepo.insertAllReturning(newPc)
      assertEquals(personRepo.count, 11L)
      assertEquals(people.size, 3)
      assertEquals(people.last.lastName, newPc.last.lastName)

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = PersonCreator(None, null, false, None)
        personRepo.insert(invalidP)

  test("update"):
    connect(ds()):
      val p = personRepo.findById(1L).get
      val updated = p.copy(firstName = None, isAdmin = false)
      personRepo.update(updated)
      assertEquals(personRepo.findById(1L).get, updated)

  test("update invalid"):
    intercept[SqlException]:
      connect(ds()):
        val p = personRepo.findById(1L).get
        val updated = p.copy(lastName = null)
        personRepo.update(updated)

  test("insertAll"):
    connect(ds()):
      val newPeople = Vector(
        PersonCreator(
          firstName = Some("Chandler"),
          lastName = "Johnsored",
          isAdmin = true,
          socialId = Some(UUID.randomUUID())
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = false,
          socialId = None
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = true,
          socialId = None
        )
      )
      personRepo.insertAll(newPeople)
      assertEquals(personRepo.count, 11L)
      assertEquals(
        personRepo.findById(11L).get.lastName,
        newPeople.last.lastName
      )

  test("updateAll"):
    connect(ds()):
      val newPeople = Vector(
        personRepo.findById(1L).get.copy(lastName = "Peterson"),
        personRepo.findById(2L).get.copy(lastName = "Moreno")
      )
      assertEquals(
        personRepo.updateAll(newPeople),
        BatchUpdateResult.Success(2)
      )
      assertEquals(personRepo.findById(1L).get, newPeople(0))
      assertEquals(personRepo.findById(2L).get, newPeople(1))

  test("transact"):
    val count = transact(ds()):
      val p = PersonCreator(
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false,
        socialId = None
      )
      personRepo.insert(p)
      personRepo.count
    assertEquals(count, 9L)

  test("transact failed"):
    val dataSource = ds()
    val p = PersonCreator(
      firstName = Some("Chandler"),
      lastName = "Brown",
      isAdmin = false,
      socialId = None
    )
    try
      transact(dataSource):
        personRepo.insert(p)
        throw RuntimeException()
      fail("should not reach")
    catch
      case _: Exception =>
        transact(dataSource):
          assertEquals(personRepo.count, 8L)

  test("custom insert"):
    connect(ds()):
      val p = PersonCreator(
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false,
        socialId = None
      )
      val update =
        sql"insert into $person ${person.insertColumns} values ($p)".update
      assertNoDiff(
        update.frag.sqlString,
        "insert into person (first_name, last_name, is_admin, social_id) values (?, ?, ?, ?)"
      )
      val rowsInserted = update.run()
      assertEquals(rowsInserted, 1)
      assertEquals(personRepo.count, 9L)
      val fetched = personRepo.findAll.last
      assertEquals(fetched.firstName, p.firstName)
      assertEquals(fetched.lastName, p.lastName)
      assertEquals(fetched.isAdmin, p.isAdmin)

  test("custom update"):
    connect(ds()):
      val p = personRepo.insertReturning(
        PersonCreator(
          firstName = Some("Chandler"),
          lastName = "Brown",
          isAdmin = false,
          socialId = Some(UUID.randomUUID())
        )
      )
      val newIsAdmin = true
      val update =
        sql"update $person set ${person.isAdmin} = $newIsAdmin where ${person.id} = ${p.id}".update
      assertNoDiff(
        update.frag.sqlString,
        "update person set is_admin = ? where id = ?"
      )
      val rowsUpdated = update.run()
      assertEquals(rowsUpdated, 1)
      assertEquals(personRepo.findById(p.id).get.isAdmin, true)

  test("custom returning a single column"):
    connect(ds()):
      val returningQuery =
        sql"insert into person (first_name, last_name, is_admin) values ('Arton', 'Senna', true) RETURNING id"
          .returning[Long]
      val personId = returningQuery.run().head
      assertEquals(personId, 9L)

  test("custom returning multiple columns"):
    connect(ds()):
      val returningQuery =
        sql"""insert into person (first_name, last_name, is_admin) values
             ('Arton', 'Senna', true),
             ('Demo', 'User', false)
             RETURNING id, created"""
          .returning[(Long, OffsetDateTime)]
      val cols = returningQuery.run()
      assertEquals(cols.map(_._1), Vector(9L, 10L))

  test("custom returning with no rows updated"):
    connect(ds()):
      val statement =
        sql"update person set first_name = 'xxx' where id = 12345 returning id"
          .returning[Long]
      val personIds = statement.run()
      assert(personIds.isEmpty)

  @SqlName("person")
  @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  case class CustomPerson(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime,
      socialId: Option[UUID]
  ) derives DbCodec

  val customPersonRepo = Repo[PersonCreator, CustomPerson, Long]

  test("count with manual table name"):
    val count = connect(ds()):
      customPersonRepo.count
    assertEquals(count, 8L)

  test(".query iterator"):
    connect(ds()):
      Using.Manager(implicit use =>
        val it = sql"SELECT * FROM person".query[Person].iterator()
        assertEquals(it.map(_.id).size, 8)
      )

  test(".returning iterator"):
    connect(ds()):
      Using.Manager(implicit use =>
        val it = sql"UPDATE person set is_admin = false RETURNING first_name"
          .returning[String]
          .iterator()
        assertEquals(it.size, 8)
      )

  @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  case class BigDec(id: Int, myBigDec: Option[BigDecimal]) derives DbCodec

  val bigDecRepo = Repo[BigDec, BigDec, Int]

  test("option of bigdecimal"):
    connect(ds()):
      val bigDec1 = bigDecRepo.findById(1).get
      assertEquals(bigDec1.myBigDec, Some(BigDecimal(123)))
      val bigDec2 = bigDecRepo.findById(2).get
      assertEquals(bigDec2.myBigDec, None)

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:15.2"))
      .createContainer()
  )

  @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  case class NoId(
      createdAt: OffsetDateTime,
      userName: String,
      userAction: String
  ) derives DbCodec

  val noIdRepo = Repo[NoId, NoId, Null]()

  test("insert NoId entities"):
    connect(ds()):
      val entity = NoId(OffsetDateTime.now, "Dan", "Fishing")
      noIdRepo.insert(entity)
      assert(noIdRepo.findAll.exists(_.userName == "Dan"))

  override def munitFixtures: Seq[Fixture[_]] =
    super.munitFixtures :+ pgContainer

  def ds(): DataSource =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    val carSql =
      Files.readString(Path.of(getClass.getResource("/pg-car.sql").toURI))
    val personSql =
      Files.readString(Path.of(getClass.getResource("/pg-person.sql").toURI))
    val bigDecSql =
      Files.readString(Path.of(getClass.getResource("/pg-bigdec.sql").toURI))
    val noIdSql =
      Files.readString(Path.of(getClass.getResource("/pg-no-id.sql").toURI))

    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(carSql)
      stmt.execute(personSql)
      stmt.execute(bigDecSql)
      stmt.execute(noIdSql)
    ).get
    ds
  end ds
end PgTests
