package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.sql.Connection
import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  ZoneOffset
}
import scala.util.Using

def dateTimeTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    Location,
    DbCodec[OffsetDateTime],
    DbCodec[LocalDate],
    DbCodec[LocalTime]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class MyTime(
      a: OffsetDateTime,
      b: LocalDate,
      c: LocalTime,
      d: LocalDateTime
  ) derives DbCodec

  val myTimeRepo = Repo[MyTime, MyTime, Null]

  val all = Vector(
    MyTime(
      a = OffsetDateTime.parse("2025-03-30T21:19:23Z"),
      b = LocalDate.parse("2025-03-30"),
      c = LocalTime.parse("05:20:04"),
      d = LocalDateTime.parse("2025-04-02T20:16:38")
    ),
    MyTime(
      a = OffsetDateTime.parse("2025-03-31T21:19:23Z"),
      b = LocalDate.parse("2025-03-31"),
      c = LocalTime.parse("05:30:04"),
      d = LocalDateTime.parse("2025-04-02T20:17:38")
    )
  )

  test("can read all JDBC 4.2 time types"):
    // https://jcp.org/aboutJava/communityprocess/maintenance/jsr221/JDBC4.2MR-Oct232013.pdf (table B-4)
    // https://jdbc.postgresql.org/documentation/query/
    xa().connect:
      assertEquals(myTimeRepo.findAll, all)

  test("can write all JDBC 4.2 time types"):
    xa().connect:
      val newTime = MyTime(
        a = OffsetDateTime.parse("2025-04-05T21:18:23Z"),
        b = LocalDate.parse("2025-04-01"),
        c = LocalTime.parse("05:20:04"),
        d = LocalDateTime.parse("2025-04-02T20:17:38")
      )
      myTimeRepo.insert(newTime)
      val res = sql"SELECT * FROM my_time ORDER BY a".query[MyTime].run()
      assertEquals(all :+ newTime, res)

end dateTimeTests
