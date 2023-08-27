import com.augustnagro.magnum.*
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager}
import java.time.OffsetDateTime
import javax.sql.DataSource
import scala.util.Properties.propOrNone
import scala.util.Using
import scala.util.Using.Manager

class H2Tests extends FunSuite:

  enum Color derives DbCodec:
    case Red, Green, Blue

  @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
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
    connect(ds()):
      assertEquals(carRepo.findAll, allCars)

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

  case class PersonCreator(
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean
  ) derives DbCodec

  @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime
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
      personRepo.delete(Person(23L, None, "", false, OffsetDateTime.now))
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

  test("insertReturning"):
    connect(ds()):
      val person = personRepo.insertReturning(
        PersonCreator(
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false
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
      val people = personRepo.insertAllReturning(newPc)
      assertEquals(personRepo.count, 11L)
      assertEquals(people.size, 3)
      assertEquals(people.last.lastName, newPc.last.lastName)

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = PersonCreator(None, null, false)
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

  test("custom insert"):
    connect(ds()):
      val p = PersonCreator(
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false
      )
      val update =
        sql"insert into $person ${person.insertColumns} values ($p)".update
      assertNoDiff(
        update.frag.sqlString,
        "insert into person (first_name, last_name, is_admin) values (?, ?, ?)"
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
          isAdmin = false
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

  lazy val h2DbPath = Files.createTempDirectory(null).toAbsolutePath

  def ds(): DataSource =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:" + h2DbPath)
    ds.setUser("sa")
    ds.setPassword("")
    val carSql =
      Files.readString(Path.of(getClass.getResource("/h2-car.sql").toURI))
    val personSql =
      Files.readString(Path.of(getClass.getResource("/h2-person.sql").toURI))
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(carSql)
      stmt.execute(personSql)
    )
    ds

end H2Tests
