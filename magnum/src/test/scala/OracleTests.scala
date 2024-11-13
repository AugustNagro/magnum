import com.augustnagro.magnum.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite}
import org.testcontainers.utility.DockerImageName
import oracle.jdbc.datasource.impl.OracleDataSource

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.UUID
import javax.sql.DataSource
import scala.util.Using

class OracleTests extends FunSuite, TestContainersFixtures:

  /*
  Immutable Repo Tests
   */

  enum Color derives DbCodec:
    case Red, Green, Blue

  @Table(OracleDbType, SqlNameMapper.CamelToSnakeCase)
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

  test("findById"):
    connect(ds()):
      assertEquals(carRepo.findById(3L).get, allCars.last)
      assertEquals(carRepo.findById(4L), None)

  test("findAllByIds"):
    intercept[UnsupportedOperationException]:
      connect(ds()):
        assertEquals(
          carRepo.findAllById(Vector(1L, 3L)).map(_.id),
          Vector(1L, 3L)
        )

  test("serializable transaction"):
    transact(ds(), withSerializable):
      assertEquals(carRepo.count, 3L)

  private def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

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
      isAdmin: String,
      socialId: Option[UUID]
  ) derives DbCodec

  @Table(OracleDbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: String,
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
      personRepo.delete(Person(23L, None, "", "N", OffsetDateTime.now, None))
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
          isAdmin = "N",
          socialId = Some(UUID.randomUUID())
        )
      )
      personRepo.insert(
        PersonCreator(
          firstName = None,
          lastName = "Prince",
          isAdmin = "Y",
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
          isAdmin = "N",
          socialId = Some(UUID.randomUUID())
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
          isAdmin = "Y",
          socialId = Some(UUID.randomUUID())
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = "N",
          socialId = None
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = "Y",
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
        val invalidP = PersonCreator(None, null, "N", None)
        personRepo.insert(invalidP)

  test("update"):
    connect(ds()):
      val p = personRepo.findById(1L).get
      val updated = p.copy(firstName = None)
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
          isAdmin = "Y",
          socialId = Some(UUID.randomUUID())
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = "N",
          socialId = None
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = "Y",
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
        isAdmin = "N",
        socialId = Some(UUID.randomUUID())
      )
      personRepo.insert(p)
      personRepo.count
    assertEquals(count, 9L)

  test("transact failed"):
    val dataSource = ds()
    val p = PersonCreator(
      firstName = Some("Chandler"),
      lastName = "Brown",
      isAdmin = "N",
      socialId = Some(UUID.randomUUID())
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
        isAdmin = "N",
        socialId = Some(UUID.randomUUID())
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
      val fetched = personRepo.findAll.find(_.firstName == p.firstName).get
      assertEquals(fetched.lastName, p.lastName)
      assertEquals(fetched.isAdmin, p.isAdmin)

  test("custom update"):
    connect(ds()):
      val p = personRepo.insertReturning(
        PersonCreator(
          firstName = Some("Chandler"),
          lastName = "Brown",
          isAdmin = "N",
          socialId = Some(UUID.randomUUID())
        )
      )
      val newIsAdmin = "Y"
      val update =
        sql"update $person set ${person.isAdmin} = $newIsAdmin where ${person.id} = ${p.id}".update
      assertNoDiff(
        update.frag.sqlString,
        "update person set is_admin = ? where id = ?"
      )
      val rowsUpdated = update.run()
      assertEquals(rowsUpdated, 1)
      assertEquals(personRepo.findById(p.id).get.isAdmin, "Y")

  test("embed Frag into Frag"):
    def findPersonCnt(filter: Frag, limit: Long = 1)(using DbCon): Int =
      val offsetFrag = sql"OFFSET 0 ROWS"
      val limitFrag = sql"FETCH NEXT $limit ROWS ONLY"
      sql"SELECT count(*) FROM person WHERE $filter $offsetFrag $limitFrag"
        .query[Int]
        .run()
        .head
    val isAdminFrag = sql"is_admin = 'Y'"
    connect(ds()):
      val johnCnt = findPersonCnt(sql"$isAdminFrag AND first_name = 'John'", 2)
      assertEquals(johnCnt, 2)

  val oracleContainer = ForAllContainerFixture(
    OracleContainer
      .Def(dockerImageName =
        DockerImageName.parse(
          "gvenzl/oracle-xe:21.3.0"
        )
      )
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ oracleContainer

  def ds(): DataSource =
    val oracle = oracleContainer()
    val ds = OracleDataSource()
    ds.setURL(oracle.jdbcUrl)
    ds.setUser(oracle.username)
    ds.setPassword(oracle.password)
    // oracle doesn't support drop if exists,
    // or multi-statement queries
    Using
      .Manager(use =>
        val con = use(ds.getConnection())
        val stmt = use(con.createStatement())
        try stmt.execute("drop table car")
        catch case _ => ()
        stmt.execute(
          """create table car (
          |  model varchar2(50) not null,
          |  id number generated always as identity,
          |  top_speed number,
          |  vin number,
          |  color varchar2(50) not null check (color in ('Red', 'Green', 'Blue'))
          |)""".stripMargin
        )
        stmt.execute(
          """insert into car (model, top_speed, vin, color)
          |values ('McLaren Senna', 208, 123, 'Red')""".stripMargin
        )
        stmt.execute(
          """insert into car (model, top_speed, vin, color)
          |values ('Ferrari F8 Tributo', 212, 124, 'Green')""".stripMargin
        )
        stmt.execute(
          """insert into car (model, top_speed, vin, color)
          |values ('Aston Martin Superleggera', 211, null, 'Blue')""".stripMargin
        )
        try stmt.execute("drop table person")
        catch case _ => ()
        stmt.execute(
          """create table person (
          |    id number generated always as identity,
          |    first_name varchar2(50),
          |    last_name varchar2(50) not null,
          |    is_admin varchar2(1) not null,
          |    created timestamp default current_timestamp,
          |    social_id varchar2(36)
          |)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('George', 'Washington', 'Y', current_timestamp, 'd06443a6-3efb-46c4-a66a-a80a8a9a5388')""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('Alexander', 'Hamilton', 'Y', current_timestamp, '529b6c6d-7228-4da5-81d7-13b706f78ddb')""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('John', 'Adams', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('Benjamin', 'Franklin', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('John', 'Jay', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('Thomas', 'Jefferson', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |('James', 'Madison', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created, social_id) values
          |(null, 'Nagro', 'N', current_timestamp, null)""".stripMargin
        )
      )
      .get
    ds
  end ds
end OracleTests
