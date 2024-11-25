package shared

import com.augustnagro.magnum.{DbCodec, DbType, Transactor}
import munit.{FunSuite, Location}

import java.time.OffsetDateTime
import java.util.UUID

def sharedTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime],
    DbCodec[BigDecimal]
): Unit =
  immutableRepoTests(suite, dbType, xa)
  repoTests(suite, dbType, xa)
  entityCreatorTests(suite, dbType, xa)
  specTests(suite, dbType, xa)
  sqlNameTests(suite, dbType, xa)
  noIdTests(suite, dbType, xa)
  embeddedFragTests(suite, dbType, xa)
  bigDecTests(suite, dbType, xa)
  optionalProductTests(suite, dbType, xa)
