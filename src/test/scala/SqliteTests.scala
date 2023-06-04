import com.augustnagro.magnum.*
import munit.FunSuite
import org.sqlite.SQLiteDataSource

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.{LocalDateTime, OffsetDateTime}
import javax.sql.DataSource
import scala.util.Using
import scala.util.Using.Manager

class SqliteTests /* extends FunSuite:

  /*
  Immutable Repo Tests
   */

  case class Car(model: String, @Id id: Long, topSpeed: Int, vin: Option[Int])
      derives DbCodec

  val carSchema = DbSchema[Car, Car, Long](
    SqliteDbType,
    SqlNameMapper.CamelToSnakeCase
  )

  val carRepo = ImmutableRepo(carSchema)

  val allCars = Vector(
    Car("McLaren Senna", 1L, 208, Some(123)),
    Car("Ferrari F8 Tributo", 2L, 212, Some(124)),
    Car("Aston Martin Superleggera", 3L, 211, None)
  )

  test("count"):
    val count = connect(ds()):
      carRepo.count
    assertEquals(count, 3L)

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
      val spec = Spec(carSchema)
        .where(sql"${carSchema.topSpeed} > 211")
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

  test("repeatable read transaction"):
    transact(ds(), withRepeatableRead):
      assertEquals(carRepo.count, 3L)

  private def withRepeatableRead(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)

  test("select query"):
    connect(ds()):
      val car = carSchema
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"

      assertNoDiff(
        query.sqlString,
        "select model, id, top_speed, vin from car where top_speed > ?"
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
        query.sqlString,
        "select c.model, c.id, c.top_speed, c.vin from car c where c.top_speed > ?"
      )
      assertEquals(query.run[Car], allCars.tail)

  test("reads null int as None and not Some(0)"):
    connect(ds()):
      assertEquals(carRepo.findById(3L).get.vin, None)

  /*
  Repo Tests
   */
  case class PersonCreator(
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean
  )

  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: String
  )

  val person = DbSchema[PersonCreator, Person, Long](
    SqliteDbType,
    SqlNameMapper.CamelToSnakeCase
  )

  // aliases should not affect generated queries
  val personRepo = Repo(person.alias("p"))

  test("delete"):
    connect(ds()):
      val p = personRepo.findById(1L).get
      personRepo.delete(p)
      assertEquals(personRepo.findById(1L), None)

  test("delete invalid"):
    connect(ds()):
      personRepo.delete(
        Person(23L, None, "", false, LocalDateTime.now.toString)
      )
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
      personRepo.deleteAll(Vector(p1, p2, p3))
      assertEquals(6L, personRepo.count)

  test("deleteAllById"):
    connect(ds()):
      personRepo.deleteAllById(Vector(1L, 2L, 1L))
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
          isAdmin = false
        )
      )
      personRepo.insert(
        PersonCreator(
          firstName = None,
          lastName = "Prince",
          isAdmin = true
        )
      )
      assertEquals(personRepo.count, 10L)
      assertEquals(personRepo.findById(9L).get.lastName, "Smith")

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = PersonCreator(None, null, false)
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
          isAdmin = true
        ),
        PersonCreator(
          firstName = None,
          lastName = "Odysseus",
          isAdmin = false
        ),
        PersonCreator(
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = true
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
      personRepo.updateAll(newPeople)
      assertEquals(personRepo.findById(1L).get, newPeople(0))
      assertEquals(personRepo.findById(2L).get, newPeople(1))

  test("transact"):
    val count = transact(ds()):
      val p = PersonCreator(
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false
      )
      personRepo.insert(p)
      personRepo.count
    assertEquals(count, 9L)

  test("transact failed"):
    val dataSource = ds()
    val p = PersonCreator(
      firstName = Some("Chandler"),
      lastName = "Brown",
      isAdmin = false
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

  lazy val sqliteDbPath = Files.createTempFile(null, ".db").toAbsolutePath

  def ds(): DataSource =
    val ds = SQLiteDataSource()
    ds.setUrl("jdbc:sqlite:" + sqliteDbPath)
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute("drop table if exists car")
      stmt.execute(
        """create table car (
          |    model text not null,
          |    id integer primary key,
          |    top_speed integer,
          |    vin integer
          |)""".stripMargin
      )
      stmt.execute(
        """insert into car (model, top_speed, vin) values
          |('McLaren Senna', 208, 123),
          |('Ferrari F8 Tributo', 212, 124),
          |('Aston Martin Superleggera', 211, null)""".stripMargin
      )
      stmt.execute("drop table if exists person")
      stmt.execute(
        """create table person (
          |    id integer primary key,
          |    first_name text,
          |    last_name text not null,
          |    is_admin integer not null,
          |    created text default(datetime())
          |)""".stripMargin
      )
      stmt.execute(
        """insert into person (first_name, last_name, is_admin, created) values
          |('George', 'Washington', true, datetime()),
          |('Alexander', 'Hamilton', true, datetime()),
          |('John', 'Adams', true, datetime()),
          |('Benjamin', 'Franklin', true, datetime()),
          |('John', 'Jay', true, datetime()),
          |('Thomas', 'Jefferson', true, datetime()),
          |('James', 'Madison', true, datetime()),
          |(null, 'Nagro', false, datetime())""".stripMargin
      )
    ).get
    ds
*/