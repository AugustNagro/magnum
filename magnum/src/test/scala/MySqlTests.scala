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
import scala.util.chaining.*
import java.sql.Types
import java.sql.ResultSet
import java.sql.PreparedStatement
import java.nio.ByteBuffer
import java.nio.ByteOrder

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
      "/mysql/big-dec.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement())
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(ds)
  end xa

  test("find by coordinates events"):
    xa().connect:

      val area = Area(Coordinates(33.17654, 56.23213), Coordinates(32.17654, 51.23213))

      val frag = sql"""
        SELECT * FROM events WHERE ST_Contains(
          ST_MakeEnvelope(${area.from}, ${area.to}),
          coordintates
        )
      """

      assert(frag.sqlString.contains("ST_PointFromWKB"))

  object WKBConstants:
    val pointType: Int = 1
    val wkbNDR: Byte = 1

  case class Area(from: Coordinates, to: Coordinates)

  case class Coordinates(longitude: Double, latitude: Double)

  given DbCodec[Coordinates] with
    override def cols: IArray[Int] = IArray(Types.BINARY)
    override def queryRepr: String = "ST_PointFromWKB(?)"

    override def readSingle(rs: ResultSet, pos: Int): Coordinates =
      read(rs.getBytes(pos))

    override def readSingleOption(
        resultSet: ResultSet,
        pos: Int
    ): Option[Coordinates] =
      Option(readSingle(resultSet, pos))

    override def writeSingle(
        coordinates: Coordinates,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      val byteBuffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
      byteBuffer.put(WKBConstants.wkbNDR)
      byteBuffer.putInt(WKBConstants.pointType)
      byteBuffer.putDouble(coordinates.latitude)
      byteBuffer.putDouble(coordinates.longitude)

      ps.setBytes(pos, byteBuffer.array())

    private def read(bytes: Array[Byte]) =
      val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val latitude = byteBuffer.getDouble(9)
      val longitude = byteBuffer.getDouble(17)
      Coordinates(latitude, longitude)

end MySqlTests
