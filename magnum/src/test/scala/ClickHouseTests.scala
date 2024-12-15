import com.augustnagro.magnum.common.*
import com.clickhouse.jdbc.ClickHouseDataSource
import com.dimafeng.testcontainers.ClickHouseContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.util.Using

class ClickHouseTests extends FunSuite, TestContainersFixtures:

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
        DockerImageName.parse("clickhouse/clickhouse-server:24.3.12.75")
      )
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ clickHouseContainer

  def xa(): Transactor =
    val clickHouse = clickHouseContainer()
    val ds = ClickHouseDataSource(clickHouse.jdbcUrl)
    val tableDDLs = Vector(
      "clickhouse/car.sql",
      "clickhouse/no-id.sql",
      "clickhouse/person.sql",
      "clickhouse/big-dec.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))
    Using
      .Manager(use =>
        val con = use(ds.getConnection)
        val stmt = use(con.createStatement)
        for ddl <- tableDDLs do stmt.execute(ddl)
      )
      .get
    Transactor(ds)
end ClickHouseTests
