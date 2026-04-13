package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.time.OffsetDateTime

def compositeIdTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using
    Location,
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class CompositeId(
      @Id firstId: Long,
      @Id secondId: Long,
      createdAt: OffsetDateTime,
      userName: String,
      userAction: String
  ) derives DbCodec

  val compositeIdRepo = Repo[CompositeId, CompositeId, (Long, Long)]()

  test("insert CompositeId entities"):
    xa().connect:
      val entity = CompositeId(2, 2, OffsetDateTime.now, "Dan", "Fishing")
      compositeIdRepo.insert(entity)
      assert(compositeIdRepo.findAll.exists(_.userName == "Dan"))
end compositeIdTests
