import com.augustnagro.magnum.*
import munit.FunSuite

import java.nio.file.{Files, Path}
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

import java.time.OffsetDateTime
import scala.util.Using

class RepoTests extends FunSuite {

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

  val person = DbSchema[PersonCreator, Person, Long](CamelToSnakeCase)

  val repo = Repo(person)

  test("delete") {
    connect(ds()) {
      repo.deleteById(1L)
      repo.deleteById(2L)
      repo.deleteById(1L)
      assertEquals(repo.findAll.size, 6)
    }
  }

  test("truncate") {
    connect(ds()) {
      repo.truncate()
      assertEquals(repo.count, 0L)
    }
  }

  test("deleteAllById") {
    connect(ds()) {
      repo.deleteAllById(Vector(1L, 2L, 1L))
      assertEquals(6L, repo.count)
    }
  }

  test("insert") {
    connect(ds()) {
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
    }
  }

  test("insert invalid") {
    connect(ds()) {
      val invalidP = PersonCreator(None, null, false)
      repo.insert(invalidP)
    }
  }

  private def ds(): DataSource =
    val ds = PGSimpleDataSource()
    ds.setServerNames(Array(PgConfig.Db.host))
    ds.setDatabaseName(PgConfig.Db.name)
    ds.setUser(PgConfig.Db.user)
    ds.setPassword(PgConfig.Db.password)
    val testSql =
      Files.readString(Path.of(getClass.getResource("/person.sql").toURI))
    Using.resource(ds.getConnection)(con =>
      con.prepareStatement(testSql).execute()
    )
    ds

}
