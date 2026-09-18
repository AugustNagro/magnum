import com.augustnagro.magnum.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import com.dimafeng.testcontainers.MSSQLServerContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import com.microsoft.sqlserver.jdbc.SQLServerDataSource
import munit.{AnyFixture, FunSuite}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.file.{Files, Path}
import scala.util.Using.Manager

class MsSqlTests extends FunSuite, TestContainersFixtures:

  sharedTests(this, MsSqlDbType, xa)


  // SQL Server caps a statement at 2100 parameters, so findAllById splits
  // long id lists across several statements. 2500 ids forces two round trips.
  test("findAllById chunks id lists over the parameter limit"):
    @Table(MsSqlDbType, SqlNameMapper.CamelToSnakeCase)
    case class Car(
        model: String,
        @Id id: Long,
        topSpeed: Int,
        @SqlName("vin") vinNumber: Option[Int],
        color: shared.Color,
        created: java.time.OffsetDateTime
    ) derives DbCodec

    val carRepo = ImmutableRepo[Car, Long]
    xa().connect:
      val ids = (1L to 2500L).toVector
      assert(ids.size > 2000)
      val found = carRepo.findAllById(ids)
      assertEquals(found.map(_.id).sorted, Vector(1L, 2L, 3L))

  val mssqlContainer = ForAllContainerFixture(
    MSSQLServerContainer
      .Def(dockerImageName =
        DockerImageName.parse("mcr.microsoft.com/mssql/server:2025-latest")
      )
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ mssqlContainer

  def xa(): Transactor =
    val mssql = mssqlContainer()
    val ds = SQLServerDataSource()
    ds.setURL(mssql.jdbcUrl)
    ds.setUser(mssql.username)
    ds.setPassword(mssql.password)
    val tableDDLs = Vector(
      "/mssql/car.sql",
      "/mssql/person.sql",
      "/mssql/my-user.sql",
      "/mssql/no-id.sql",
      "/mssql/big-dec.sql",
      "/mssql/my-time.sql",
      "/mssql/comp-id.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))

    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(ds)
  end xa
end MsSqlTests
