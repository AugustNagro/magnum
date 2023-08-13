import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.FunSuite
import org.testcontainers.utility.DockerImageName
import oracle.jdbc.datasource.impl.OracleDataSource

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.OffsetDateTime
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

  object carRepo extends ImmutableRepo[Car, Long]:
    def customSelect(using DbCon): Car =
      val sql = sql"select ${*} from $table where $id = 1"
      sql.query[Car].run().head

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
        .where(sql"top_speed > $topSpeed")
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
      val query = sql"select * from car where top_speed > $minSpeed".query[Car]
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
      val tuples = sql"select model, color from car where id = 2"
        .query[(String, Color)]
        .run()
      assertEquals(tuples, Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    connect(ds()):
      assertEquals(carRepo.findById(3L).get.vinNumber, None)

  test("custom select"):
    connect(ds()):
      assertEquals(carRepo.customSelect, allCars.head)

  /*
  Repo Tests
   */
  case class PersonCreator(
      firstName: Option[String],
      lastName: String,
      isAdmin: String
  ) derives DbCodec

  @Table(OracleDbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: String,
      created: OffsetDateTime
  ) derives DbCodec

  object personRepo extends Repo[PersonCreator, Person, Long]:
    def customInsert(p: PersonCreator)(using DbCon): Unit =
      val sql = sql"insert into $table $insertColumns values ($p)"
      sql.update.run()

    def customUpdate(personId: Long, isAdmin: "Y" | "N")(using DbCon): Unit =
      val sql =
        sql"update $table set is_admin = ${isAdmin: String} where $id = $personId"
      sql.update.run()

  test("delete"):
    connect(ds()):
      val p = personRepo.findById(1L).get
      personRepo.delete(p)
      assertEquals(personRepo.findById(1L), None)

  test("delete invalid"):
    connect(ds()):
      personRepo.delete(Person(23L, None, "", "N", OffsetDateTime.now))
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
          isAdmin = "N"
        )
      )
      personRepo.insert(
        PersonCreator(
          firstName = None,
          lastName = "Prince",
          isAdmin = "Y"
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
          isAdmin = "N"
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
          isAdmin = "Y"
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = "N"
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = "Y"
        )
      )
      val people = personRepo.insertAllReturning(newPc)
      assertEquals(personRepo.count, 11L)
      assertEquals(people.size, 3)
      assertEquals(people.last.lastName, newPc.last.lastName)

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = PersonCreator(None, null, "N")
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
          isAdmin = "Y"
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = "N"
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = "Y"
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
        isAdmin = "N"
      )
      personRepo.insert(p)
      personRepo.count
    assertEquals(count, 9L)

  test("transact failed"):
    val dataSource = ds()
    val p = PersonCreator(
      firstName = Some("Chandler"),
      lastName = "Brown",
      isAdmin = "N"
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
        isAdmin = "N"
      )
      personRepo.customInsert(p)
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
          isAdmin = "N"
        )
      )
      personRepo.customUpdate(p.id, isAdmin = "Y")
      assertEquals(personRepo.findById(1L).get.isAdmin, "Y")

  val oracleContainer = ForAllContainerFixture(
    OracleContainer
      .Def(dockerImageName =
        DockerImageName.parse(
          "gvenzl/oracle-xe:21.3.0"
        )
      )
      .createContainer()
  )

  override def munitFixtures: Seq[Fixture[_]] =
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
          |    created timestamp default current_timestamp
          |)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('George', 'Washington', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('Alexander', 'Hamilton', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('John', 'Adams', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('Benjamin', 'Franklin', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('John', 'Jay', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('Thomas', 'Jefferson', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |('James', 'Madison', 'Y', current_timestamp)""".stripMargin
        )
        stmt.execute(
          """insert into person (first_name, last_name, is_admin, created) values
          |(null, 'Nagro', 'N', current_timestamp)""".stripMargin
        )
      )
      .get
    ds
