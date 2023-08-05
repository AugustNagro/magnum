import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.FunSuite
import org.testcontainers.utility.DockerImageName
import org.postgresql.ds.PGSimpleDataSource
import org.postgresql.geometric.*
import com.augustnagro.magnum.*
import com.augustnagro.magnum.pg.PgCodec.given
import org.postgresql.util.PGInterval

import java.nio.file.{Files, Path}
import java.time.{OffsetDateTime, ZoneOffset}
import java.util
import java.util.Objects
import javax.sql.DataSource
import scala.util.Using.Manager

class PgCodecTests extends FunSuite, TestContainersFixtures:

  @Table(PostgresDbType)
  @SqlName("mag_user")
  case class MagUser(
      @Id id: Long,
      name: String,
      friends: Vector[String],
      matrix: IArray[IArray[Int]],
      test: IArray[Int],
      dates: IArray[OffsetDateTime],
      bx: PGbox,
      c: PGcircle,
      iv: PGInterval
  ) derives DbCodec:
    override def equals(obj: Any): Boolean =
      obj match
        case u: MagUser =>
          id == u.id && name == u.name && friends == u.friends &&
          Objects.deepEquals(matrix, u.matrix) &&
          Objects.deepEquals(test, u.test) &&
          Objects.deepEquals(dates, u.dates) &&
          bx == u.bx && c == u.c && iv == u.iv
        case _ => false

  val userRepo = Repo[MagUser, MagUser, Long]

  val allUsers = Vector(
    MagUser(
      id = 1L,
      name = "Abby",
      friends = Vector("Jane", "Mary"),
      matrix = IArray(IArray(1, 2), IArray(3, 4), IArray(5, 6)),
      test = IArray(1),
      dates = IArray(
        OffsetDateTime.parse("2023-07-30T12:21:36Z"),
        OffsetDateTime.parse("2023-07-30T12:21:37Z")
      ),
      bx = PGbox(1, 2, 3, 4),
      c = PGcircle(1, 2, 3),
      iv = PGInterval("1 hour")
    ),
    MagUser(
      id = 2L,
      name = "Jacob",
      friends = Vector("Grace", "Aubrey"),
      matrix = IArray(IArray(7, 8), IArray(9, 10)),
      test = IArray.emptyIntIArray,
      dates = IArray.empty,
      bx = PGbox(5, 6, 7, 8),
      c = PGcircle(4, 5, 6),
      iv = PGInterval("2 days")
    )
  )

  test("select all"):
    connect(ds()):
      assertEquals(userRepo.findAll, allUsers)

  test("insert"):
    connect(ds()):
      val u = MagUser(
        id = 3L,
        name = "Matt",
        friends = Vector.empty,
        matrix = IArray(IArray(1, 2), IArray(3, 4)),
        test = IArray(4),
        dates = IArray(OffsetDateTime.parse("2023-07-30T13:57:29.059335Z")),
        bx = PGbox(1, 2, 3, 4),
        c = PGcircle(1, 1, 1),
        iv = PGInterval("1 minute")
      )
      userRepo.insert(u)
      val dbU = userRepo.findById(3L).get
      assertEquals(dbU, u)

  test("update arrays"):
    connect(ds()):
      val newMatrix = IArray(IArray(0, 0), IArray(0, 9))
      sql"UPDATE mag_user SET matrix = $newMatrix WHERE id = 2".update
        .run()
      val newUser = userRepo.findById(2L).get
      assert(Objects.deepEquals(newUser.matrix, newMatrix))

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:15.2"))
      .createContainer()
  )

  override def munitFixtures: Seq[Fixture[_]] =
    super.munitFixtures :+ pgContainer

  def ds(): DataSource =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    val userSql =
      Files.readString(Path.of(getClass.getResource("/pg-user.sql").toURI))
    Manager { use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(userSql)
    }.get
    ds
