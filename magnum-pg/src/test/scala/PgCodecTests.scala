import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.FunSuite
import org.testcontainers.utility.DockerImageName
import org.postgresql.ds.PGSimpleDataSource
import org.postgresql.geometric.*
import com.augustnagro.magnum.*
import com.augustnagro.magnum.pg.PgCodec.given
import com.augustnagro.magnum.pg.enums.PgEnumToScalaEnumSqlArrayCodec
import org.postgresql.util.PGInterval

import java.nio.file.{Files, Path}
import java.time.{OffsetDateTime, ZoneOffset}
import java.util
import java.util.Objects
import javax.sql.DataSource
import scala.util.Using.Manager

class PgCodecTests extends FunSuite, TestContainersFixtures:
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
      iv = PGInterval("1 hour"),
      l = PGline(1, 1, 1),
      lSeg = PGlseg(1, 1, 2, 2),
      p = PGpath(Array(PGpoint(1, 1), PGpoint(2, 2)), true),
      pnt = PGpoint(1, 1),
      poly = PGpolygon(Array(PGpoint(0, 0), PGpoint(-1, 1), PGpoint(1, 1))),
      colors = List(Color.RedOrange, Color.Green),
      colorMap = List(
        Vector(Color.RedOrange, Color.RedOrange),
        Vector(Color.Green, Color.Green)
      ),
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
      iv = PGInterval("2 days"),
      l = PGline(2, 2, 2),
      lSeg = PGlseg(2, 2, 3, 3),
      p = PGpath(Array(PGpoint(2, 2), PGpoint(3, 3)), true),
      pnt = PGpoint(2, 2),
      poly = PGpolygon(Array(PGpoint(0, 0), PGpoint(-1, -1), PGpoint(1, -1))),
      colors = List(Color.Green, Color.Blue),
      colorMap = List(
        Vector(Color.RedOrange, Color.Green),
        Vector(Color.Green, Color.Blue)
      )
    )
  )

  val carRepo = Repo[MagCar, MagCar, Long]

  val allCars = Vector(
    MagCar(
      id = 1,
      textColors = Seq(Color.RedOrange, Color.Green),
      textColorMap = Vector(
        List(Color.RedOrange, Color.RedOrange),
        List(Color.Green, Color.Green)
      )
    ),
    MagCar(
      id = 2,
      textColors = Seq(Color.Green, Color.Blue),
      textColorMap = Vector(
        List(Color.RedOrange, Color.Green),
        List(Color.Green, Color.Blue)
      )
    )
  )

  test("select all MagUser"):
    connect(ds()):
      assertEquals(userRepo.findAll, allUsers)

  test("select all MagCar"):
    connect(ds()):
      assertEquals(carRepo.findAll, allCars)

  test("insert MagUser"):
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
        iv = PGInterval("1 minute"),
        l = PGline(3, 4, 5),
        lSeg = PGlseg(0, 0, -1, -1),
        p = PGpath(Array(PGpoint(3, 3), PGpoint(4, 4)), true),
        pnt = PGpoint(3, 4),
        poly = PGpolygon(Array(PGpoint(0, 0), PGpoint(-1, 1), PGpoint(1, 1))),
        colors = List(Color.Blue),
        colorMap = List(Vector(Color.Blue), Vector(Color.Green))
      )
      userRepo.insert(u)
      val dbU = userRepo.findById(3L).get
      assertEquals(dbU, u)

  test("insert MagCar"):
    connect(ds()):
      val c = MagCar(
        id = 3L,
        textColors = Vector(Color.RedOrange, Color.RedOrange),
        textColorMap = Vector(
          List(Color.RedOrange, Color.RedOrange),
          List(Color.RedOrange, Color.RedOrange)
        )
      )
      carRepo.insert(c)
      val dbC = carRepo.findById(3L).get
      assertEquals(dbC, c)

  test("update MagUser arrays"):
    connect(ds()):
      val newMatrix = IArray(IArray(0, 0), IArray(0, 9))
      sql"UPDATE mag_user SET matrix = $newMatrix WHERE id = 2".update
        .run()
      val newUser = userRepo.findById(2L).get
      assert(Objects.deepEquals(newUser.matrix, newMatrix))

  test("update MagCar arrays"):
    connect(ds()):
      val newTextColorMap =
        Vector(List(Color.Blue, Color.Blue), List(Color.Blue, Color.Blue))
      sql"UPDATE mag_car SET text_color_map = $newTextColorMap WHERE id = 2".update
        .run()
      val newCar = carRepo.findById(2L).get
      assertEquals(newCar.textColorMap, newTextColorMap)

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
    val carSql =
      Files.readString(Path.of(getClass.getResource("/pg-car.sql").toURI))
    Manager { use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(userSql)
      stmt.execute(carSql)
    }.get
    ds
end PgCodecTests
