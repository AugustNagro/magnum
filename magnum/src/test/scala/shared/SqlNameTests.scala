package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.time.OffsetDateTime

def sqlNameTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    Location
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
