import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.charset.StandardCharsets
import scala.util.Using
import scala.util.Using.Manager

class PgTests extends FunSuite, TestContainersFixtures:

  sharedTests(this, PostgresDbType, xa)

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:18.6"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ pgContainer

  def xa(): Transactor =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    val tableDDLs = Vector(
      "/pg/car.sql",
      "/pg/person.sql",
      "/pg/my-user.sql",
      "/pg/no-id.sql",
      "/pg/big-dec.sql",
      "/pg/my-time.sql",
      "/pg/comp-id.sql"
    ).map(p =>
      Using.resource(getClass.getResourceAsStream(p))(stream =>
        String(stream.readAllBytes(), StandardCharsets.UTF_8)
      )
    )

    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(ds)
  end xa
end PgTests
