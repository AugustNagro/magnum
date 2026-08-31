import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.ClickHouseContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager}
import java.time.{LocalDateTime, LocalTime}
import java.util.logging.Logger
import java.util.{Properties, UUID}
import javax.sql.DataSource
import scala.util.Using

private final class DriverManagerDataSource(url: String, properties: Properties)
    extends DataSource:
  override def getConnection: Connection =
    DriverManager.getConnection(url, properties)

  override def getConnection(username: String, password: String): Connection =
    DriverManager.getConnection(url, username, password)

  override def getLogWriter: PrintWriter = DriverManager.getLogWriter
  override def setLogWriter(writer: PrintWriter): Unit =
    DriverManager.setLogWriter(writer)
  override def getLoginTimeout: Int = DriverManager.getLoginTimeout
  override def setLoginTimeout(seconds: Int): Unit =
    DriverManager.setLoginTimeout(seconds)
  override def getParentLogger: Logger = Logger.getLogger("com.clickhouse.jdbc")
  override def unwrap[T](iface: Class[T]): T =
    if iface.isInstance(this) then iface.cast(this)
    else throw java.sql.SQLException(s"Not a wrapper for ${iface.getName}")
  override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)

class ClickHouseTests extends FunSuite, TestContainersFixtures:

  given DbCodec[LocalTime] =
    DbCodec[String].biMap(LocalTime.parse, _.toString)

  given DbCodec[LocalDateTime] =
    DbCodec[String].biMap(
      value => LocalDateTime.parse(value.replace(' ', 'T')),
      _.toString
    )

  sharedTests(this, ClickhouseDbType, xa)

  test("only allows EC =:= E"):
    intercept[IllegalArgumentException]:
      case class UserCreator(name: String) derives DbCodec
      @Table(ClickhouseDbType)
      case class User(id: UUID, name: String) derives DbCodec
      val repo = Repo[UserCreator, User, UUID]

  val clickHouseContainer = ForAllContainerFixture(
    ClickHouseContainer
      .Def(dockerImageName =
        DockerImageName.parse("clickhouse/clickhouse-server:26.3.25.2")
      )
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ clickHouseContainer

  def xa(): Transactor =
    val clickHouse = clickHouseContainer()
    val props = Properties()
    props.put("user", clickHouse.username)
    props.put("password", clickHouse.password)
    props.put("jdbc_ignore_unsupported_values", "true")
    val ds = DriverManagerDataSource(clickHouse.jdbcUrl, props)
    val tableStatements = Vector(
      "clickhouse/car.sql",
      "clickhouse/no-id.sql",
      "clickhouse/person.sql",
      "clickhouse/big-dec.sql",
      "clickhouse/my-time.sql"
    ).flatMap(p =>
      Using
        .resource(getClass.getResourceAsStream(p))(stream =>
          String(stream.readAllBytes(), StandardCharsets.UTF_8)
        )
        .split(';')
        .map(_.trim)
        .filter(_.nonEmpty)
    )
    Using
      .Manager(use =>
        val con = use(ds.getConnection)
        val stmt = use(con.createStatement)
        for sql <- tableStatements do stmt.execute(sql)
        stmt.execute(
          "alter table person modify setting enable_block_number_column = 1"
        )
        stmt.execute(
          "alter table person modify setting enable_block_offset_column = 1"
        )
      )
      .get
    Transactor(ds)
  end xa
end ClickHouseTests
