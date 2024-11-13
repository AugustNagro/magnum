import com.augustnagro.magnum.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import munit.FunSuite
import org.sqlite.SQLiteDataSource

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.{LocalDateTime, OffsetDateTime}
import java.util.UUID
import javax.sql.DataSource
import scala.util.Using
import scala.util.Using.Manager

class SqliteTests extends FunSuite:

  /*
  Immutable Repo Tests
   */

  enum Color derives DbCodec:
    case Red, Green, Blue

  @Table(SqliteDbType, SqlNameMapper.CamelToSnakeCase)
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

  @Table(SqliteDbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: String,
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
      personRepo.delete(
        Person(23L, None, "", false, LocalDateTime.now.toString, None)
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
          socialId = Some(UUID.randomUUID())
        )
      )
      assertEquals(person.id, 9L)
      assertEquals(person.lastName, "Smith")

  test("insertAllReturning"):
    intercept[UnsupportedOperationException]:
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
        println(people)
        assertEquals(people.size, 3)
        assertEquals(people.last.lastName, newPc.last.lastName)

  test("custom returning"):
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
             RETURNING id"""
          .returning[Long]
      val cols = returningQuery.run()
      assertEquals(cols, Vector(9L, 10L))

  test("custom returning with no rows updated"):
    connect(ds()):
      val statement =
        sql"update person set first_name = 'xxx' where id = 12345 returning id"
          .returning[Long]
      val personIds = statement.run()
      assert(personIds.isEmpty)

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = PersonCreator(None, null, false, None)
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
      personRepo.updateAll(newPeople)
      assertEquals(personRepo.findById(1L).get, newPeople(0))
      assertEquals(personRepo.findById(2L).get, newPeople(1))

  test("transact"):
    val count = transact(ds()):
      val p = PersonCreator(
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false,
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
      isAdmin = false,
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
        isAdmin = false,
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

  test("embed Frag into Frag"):
    def findPersonCnt(filter: Frag, limit: Long = 1)(using DbCon): Int =
      val offsetFrag = sql"OFFSET 0"
      val limitFrag = sql"LIMIT $limit"
      sql"SELECT count(*) FROM person WHERE $filter $limitFrag $offsetFrag"
        .query[Int]
        .run()
        .head
    val isAdminFrag = sql"is_admin = true"
    connect(ds()):
      val johnCnt = findPersonCnt(sql"$isAdminFrag AND first_name = 'John'", 2)
      assertEquals(johnCnt, 2)

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
            |    vin integer,
            |    color text check (color in ('Red', 'Green', 'Blue')) not null
            |)""".stripMargin
      )
      stmt.execute(
        """insert into car (model, top_speed, vin, color) values
            |('McLaren Senna', 208, 123, 'Red'),
            |('Ferrari F8 Tributo', 212, 124, 'Green'),
            |('Aston Martin Superleggera', 211, null, 'Blue')""".stripMargin
      )
      stmt.execute("drop table if exists person")
      stmt.execute(
        """create table person (
            |    id integer primary key,
            |    first_name text,
            |    last_name text not null,
            |    is_admin integer not null,
            |    created text default(datetime()),
            |    social_id varchar(36)
            |)""".stripMargin
      )
      stmt.execute(
        """insert into person (first_name, last_name, is_admin, created, social_id) values
            |('George', 'Washington', true, datetime(), 'd06443a6-3efb-46c4-a66a-a80a8a9a5388'),
            |('Alexander', 'Hamilton', true, datetime(), '529b6c6d-7228-4da5-81d7-13b706f78ddb'),
            |('John', 'Adams', true, datetime(), null),
            |('Benjamin', 'Franklin', true, datetime(), null),
            |('John', 'Jay', true, datetime(), null),
            |('Thomas', 'Jefferson', true, datetime(), null),
            |('James', 'Madison', true, datetime(), null),
            |(null, 'Nagro', false, datetime(), null)""".stripMargin
      )
    ).get
    ds
  end ds
end SqliteTests
