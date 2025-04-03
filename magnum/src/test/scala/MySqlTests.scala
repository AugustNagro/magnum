import com.augustnagro.magnum.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import com.dimafeng.testcontainers.MySQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.mysql.cj.jdbc.MysqlDataSource
import munit.{AnyFixture, FunSuite, Location}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.file.{Files, Path}
import scala.util.Using
import scala.util.Using.Manager

class MySqlTests extends FunSuite, TestContainersFixtures:

  sharedTests(this, MySqlDbType, xa)

  val mySqlContainer = ForAllContainerFixture(
    MySQLContainer
      .Def(dockerImageName = DockerImageName.parse("mysql:8.0.32"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ mySqlContainer

  def xa(): Transactor =
    val mySql = mySqlContainer()
    val ds = MysqlDataSource()
    ds.setURL(mySql.jdbcUrl)
    ds.setUser(mySql.username)
    ds.setPassword(mySql.password)
    ds.setAllowMultiQueries(true)
    ds.setServerTimezone("UTC")
    val tableDDLs = Vector(
      "/mysql/car.sql",
      "/mysql/person.sql",
      "/mysql/my-user.sql",
      "/mysql/no-id.sql",
      "/mysql/big-dec.sql",
      "/mysql/my-time.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement())
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(ds)
  end xa
end MySqlTests
