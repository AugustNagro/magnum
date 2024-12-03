package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.builders.Transactor
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.DbType
import com.augustnagro.magnum.repo.{Repo, SqlNameMapper, Table}
import munit.FunSuite

def bigDecTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    munit.Location,
    DbCodec[BigDecimal]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class BigDec(id: Int, myBigDec: Option[BigDecimal]) derives DbCodec

  val bigDecRepo = Repo[BigDec, BigDec, Int]

  test("option of bigdecimal"):
    connect(xa()):
      val bigDec1 = bigDecRepo.findById(1).get
      assert(bigDec1.myBigDec == Some(BigDecimal(123)))
      val bigDec2 = bigDecRepo.findById(2).get
      assert(bigDec2.myBigDec == None)
