package shared

import com.augustnagro.magnum.*
import munit.FunSuite

import java.time.OffsetDateTime

def noIdTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    munit.Location,
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
    connect(xa()):
      val entity = NoId(OffsetDateTime.now, "Dan", "Fishing")
      noIdRepo.insert(entity)
      assert(noIdRepo.findAll.exists(_.userName == "Dan"))
end noIdTests
