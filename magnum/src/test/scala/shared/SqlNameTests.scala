package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.DbType
import com.augustnagro.magnum.repo.{Id, Repo, SqlName, SqlNameMapper, Table}
import com.augustnagro.magnum.shared.builders.Transactor
import munit.FunSuite

import java.time.OffsetDateTime

def sqlNameTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    munit.Location
): Unit =
  import suite.*

  @SqlName("car")
  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class CustomCar(
      model: String,
      @Id id: Long,
      topSpeed: Int,
      @SqlName("vin") vinNumber: Option[Int],
      color: Color,
      created: OffsetDateTime
  ) derives DbCodec

  val customCarRepo = Repo[CustomCar, CustomCar, Long]

  test("count with manual table name"):
    val count = connect(xa())(customCarRepo.count)
    assert(count == 3L)
end sqlNameTests
