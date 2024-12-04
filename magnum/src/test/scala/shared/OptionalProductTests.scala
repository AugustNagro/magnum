package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.{ClickhouseDbType, DbType}
import com.augustnagro.magnum.repo.{Id, SqlName, SqlNameMapper, Table}
import com.augustnagro.magnum.shared.builders.Transactor
import munit.{FunSuite, Location}

import java.time.OffsetDateTime

def optionalProductTests(
    suite: FunSuite,
    dbType: DbType,
    xa: () => Transactor
)(using Location, DbCodec[BigDecimal], DbCodec[OffsetDateTime]): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Car(
      model: String,
      @Id id: Long,
      topSpeed: Int,
      @SqlName("vin") vinNumber: Option[Int],
      color: Color,
      created: OffsetDateTime
  ) derives DbCodec

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class BigDec(id: Int, myBigDec: Option[BigDecimal]) derives DbCodec

  test("left join with optional product type"):
    assume(dbType != ClickhouseDbType)
    connect(xa()):
      val res = sql"select * from car c left join big_dec bd on bd.id = c.id"
        .query[(Car, Option[BigDec])]
        .run()
      assert(res.exists((_, bigDec) => bigDec.isEmpty))
end optionalProductTests
