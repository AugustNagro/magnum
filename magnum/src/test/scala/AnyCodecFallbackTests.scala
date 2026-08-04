import com.augustnagro.magnum.*
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource

import java.math.BigInteger
import java.sql.{PreparedStatement, ResultSet, Types}

class AnyCodecFallbackTests extends FunSuite:

  lazy val xa: Transactor =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:mem:anycodec;DB_CLOSE_DELAY=-1")
    ds.setUser("sa")
    ds.setPassword("")
    Transactor(ds)

  test("interpolating a type with no DbCodec falls back to AnyCodec"):
    val n = BigInteger.valueOf(42)
    val res = connect(xa):
      sql"select $n".query[Long].run().head
    assertEquals(res, 42L)

  test("a DbCodec[Any] in scope takes precedence over the AnyCodec fallback"):
    given DbCodec[Any] with
      val cols: IArray[Int] = IArray(Types.VARCHAR)
      def readSingle(rs: ResultSet, pos: Int): Any = rs.getString(pos)
      def readSingleOption(rs: ResultSet, pos: Int): Option[Any] =
        Option(rs.getString(pos))
      def writeSingle(a: Any, ps: PreparedStatement, pos: Int): Unit =
        ps.setString(pos, "intercepted")
      def queryRepr: String = "?"
    val n = BigInteger.valueOf(42)
    val res = connect(xa):
      sql"select $n".query[String].run().head
    assertEquals(res, "intercepted")

end AnyCodecFallbackTests
