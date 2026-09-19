package shared

import com.augustnagro.magnum.*
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

  val noIdTableInfo = TableInfo[NoId, NoId, Null]

  test("insert NoId entities"):
    xa().connect:
      val entity = NoId(OffsetDateTime.now, "Dan", "Fishing")
      noIdRepo.insert(entity)
      assert(noIdRepo.findAll.exists(_.userName == "Dan"))

  test("select NoId usernames via TableInfo"):
    xa().connect:
      val userNames = sql"SELECT ${noIdTableInfo.userName} FROM $noIdTableInfo"
        .query[String]
        .run()
      assert(userNames.size == 3)

  test("NoId ID operations are no-ops"):
    xa().connect:
      val noId: Null = null
      val entity = NoId(OffsetDateTime.now, "unused", "unused")
      val initialCount = noIdRepo.count

      assert(!noIdRepo.existsById(noId))
      assertEquals(noIdRepo.findById(noId), None)
      noIdRepo.deleteById(noId)
      noIdRepo.delete(entity)
      assertEquals(
        noIdRepo.deleteAllById(Vector(noId)),
        BatchUpdateResult.Success(0)
      )
      assertEquals(
        noIdRepo.deleteAll(Vector(entity)),
        BatchUpdateResult.Success(0)
      )
      assertEquals(noIdRepo.count, initialCount)

end noIdTests
