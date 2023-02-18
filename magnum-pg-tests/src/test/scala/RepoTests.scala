import com.augustnagro.magnum.{CamelToSnakeCase, DbSchema}
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
