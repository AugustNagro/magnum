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

  // SQL Server's uniqueidentifier does not compare in string order: it orders
  // by byte group, last group first. It also renders back uppercase whatever
  // case went in. Both differ from the varchar(36) columns the other
  // VarCharUUIDCodec dialects use, so they are pinned here and documented in
  // the README.
  test("uniqueidentifier sorts by byte group, not lexicographically"):
    val low = java.util.UUID.fromString("00000000-0000-0000-0000-000000000002")
    val high = java.util.UUID.fromString("ffffffff-0000-0000-0000-000000000001")
    xa().connect:
      sql"""insert into person (id, first_name, last_name, is_admin, created, social_id)
            values (9, 'A', 'A', 0, sysdatetimeoffset(), $low),
                   (10, 'B', 'B', 0, sysdatetimeoffset(), $high)""".update.run()

      val sorted =
        sql"select social_id from person where id in (9, 10) order by social_id"
          .query[java.util.UUID]
          .run()
      // lexicographically `low` sorts first; uniqueidentifier puts `high` first
      assertEquals(sorted, Vector(high, low))
      assertNotEquals(sorted, sorted.sortBy(_.toString))

      val rendered =
        sql"select cast(social_id as varchar(36)) from person where id = 10"
          .query[String]
          .run()
          .head
      assertEquals(rendered, rendered.toUpperCase)

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
        DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest")
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
      "/mssql/my-time.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))

    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(ds)
  end xa
end MsSqlTests
