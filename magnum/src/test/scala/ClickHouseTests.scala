import com.augustnagro.magnum.*
import com.clickhouse.client.api.ClientConfigProperties
import com.clickhouse.jdbc.DataSourceImpl
import com.dimafeng.testcontainers.ClickHouseContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.charset.StandardCharsets
import java.time.{LocalDateTime, LocalTime}
import java.util.{Properties, UUID}
import scala.util.Using

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

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ clickHouseContainer

  def xa(): Transactor =
    val clickHouse = clickHouseContainer()
    val props = Properties()
    props.put("user", clickHouse.username)
    props.put("password", clickHouse.password)
    val ds = DataSourceImpl(clickHouse.jdbcUrl, props)
    val tableStatements = Vector(
      "clickhouse/car.sql",
      "clickhouse/no-id.sql",
      "clickhouse/person.sql",
      "clickhouse/big-dec.sql",
      "clickhouse/my-time.sql",
      "clickhouse/comp-id.sql"
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
      )
      .get
    Transactor(ds)
  end xa
end ClickHouseTests
