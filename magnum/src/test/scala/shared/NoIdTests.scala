package shared

import com.augustnagro.magnum.common.*
import munit.{FunSuite, Location}

import java.time.OffsetDateTime

def noIdTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    Location,
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class NoId(
      createdAt: OffsetDateTime,
      userName: String,
      userAction: String
  ) derives DbCodec

  val noIdRepo = Repo[NoId, NoId, Null]()

  test("insert NoId entities"):
    xa().connect:
      val entity = NoId(OffsetDateTime.now, "Dan", "Fishing")
      noIdRepo.insert(entity)
      assert(noIdRepo.findAll.exists(_.userName == "Dan"))
end noIdTests
