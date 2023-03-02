import com.augustnagro.magnum.*
import munit.FunSuite

import java.nio.file.{Files, Path}
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

import java.time.OffsetDateTime
import scala.util.Using

class RepoTests extends FunSuite:

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
      created: OffsetDateTime
  )

  val person =
    DbSchema[PersonCreator, Person, Long](SqlNameMapper.CamelToSnakeCase)

  // aliases should not affect generated queries
  val repo = Repo(person.alias("p"))

  test("delete"):
    connect(ds()):
      val p = repo.findById(1L).get
      repo.delete(p)
      assertEquals(repo.findById(1L), None)

  test("delete invalid"):
    connect(ds()):
      repo.delete(Person(23L, None, "", false, OffsetDateTime.now))
      assertEquals(8L, repo.count)

  test("deleteById"):
    connect(ds()):
      repo.deleteById(1L)
      repo.deleteById(2L)
      repo.deleteById(1L)
      assertEquals(repo.findAll.size, 6)

  test("deleteAll"):
    connect(ds()):
      val p1 = repo.findById(1L).get
      val p2 = p1.copy(id = 2L)
      val p3 = p1.copy(id = 99L)
      repo.deleteAll(Vector(p1, p2, p3))
      assertEquals(6L, repo.count)

  test("deleteAllById"):
    connect(ds()):
      repo.deleteAllById(Vector(1L, 2L, 1L))
      assertEquals(6L, repo.count)

  test("truncate"):
    connect(ds()):
      repo.truncate()
      assertEquals(repo.count, 0L)

  test("insert"):
    connect(ds()):
      repo.insert(
        PersonCreator(
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false
        )
      )
      repo.insert(
        PersonCreator(
          firstName = None,
          lastName = "Prince",
          isAdmin = true
        )
      )
      assertEquals(repo.count, 10L)
      assertEquals(repo.findById(9L).get.lastName, "Smith")

  test("insert invalid"):
    intercept[SqlException]:
      connect(ds()):
        val invalidP = PersonCreator(None, null, false)
        repo.insert(invalidP)

  test("update"):
    connect(ds()):
      val p = repo.findById(1L).get
      val updated = p.copy(firstName = None)
      repo.update(updated)
      assertEquals(repo.findById(1L).get, updated)

  test("update invalid"):
    intercept[SqlException]:
      connect(ds()):
        val p = repo.findById(1L).get
        val updated = p.copy(lastName = null)
        repo.update(updated)

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
      repo.insertAll(newPeople)
      assertEquals(repo.count, 11L)
      assertEquals(repo.findById(11L).get.lastName, newPeople.last.lastName)

  test("updateAll"):
    connect(ds()):
      val newPeople = Vector(
        repo.findById(1L).get.copy(lastName = "Peterson"),
        repo.findById(2L).get.copy(lastName = "Moreno")
      )
      repo.updateAll(newPeople)
      assertEquals(repo.findById(1L).get, newPeople(0))
      assertEquals(repo.findById(2L).get, newPeople(1))

  test("transact"):
    val count = transact(ds()):
      val p = PersonCreator(
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false
      )
      repo.insert(p)
      repo.count
    assertEquals(count, 9L)

  test("transact failed"):
    val p = PersonCreator(
      firstName = Some("Chandler"),
      lastName = "Brown",
      isAdmin = false
    )
    try
      transact(ds()):
        repo.insert(p)
        throw RuntimeException()
      fail("should not reach")
    catch
      case _: Exception =>
        transact(ds()):
          assertEquals(repo.count, 8L)

  def ds(): DataSource =
    val ds = PGSimpleDataSource()
    ds.setServerNames(Array(PgConfig.Db.host))
    ds.setDatabaseName(PgConfig.Db.name)
    ds.setUser(PgConfig.Db.user)
    ds.setPassword(PgConfig.Db.password)
    ds.setPortNumbers(Array(PgConfig.Db.port))
    val testSql =
      Files.readString(Path.of(getClass.getResource("/person.sql").toURI))
    Using.resource(ds.getConnection)(con =>
      con.prepareStatement(testSql).execute()
    )
    ds
