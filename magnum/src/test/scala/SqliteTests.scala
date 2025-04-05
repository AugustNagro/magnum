import com.augustnagro.magnum.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import munit.FunSuite
import org.sqlite.SQLiteDataSource
import shared.*

import java.nio.file.Files
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime}
import java.util.UUID
import scala.util.Using
import scala.util.Using.Manager

class SqliteTests extends FunSuite:

  given DbCodec[OffsetDateTime] =
    DbCodec[String].biMap(OffsetDateTime.parse, _.toString)

  given DbCodec[LocalDate] =
    DbCodec[String].biMap(LocalDate.parse, _.toString)

  given DbCodec[UUID] =
    DbCodec[String].biMap(UUID.fromString, _.toString)

  given DbCodec[Boolean] =
    DbCodec[Int].biMap(_ != 0, b => if b then 1 else 0)

  given DbCodec[BigDecimal] =
    DbCodec[String].biMap(BigDecimal.apply, _.toString())

  given DbCodec[LocalTime] =
    DbCodec[String].biMap(LocalTime.parse, _.toString)

  given DbCodec[LocalDateTime] =
    DbCodec[String].biMap(LocalDateTime.parse, _.toString)

  sharedTests(this, SqliteDbType, xa)

  lazy val sqliteDbPath = Files.createTempFile(null, ".db").toAbsolutePath

  def xa(): Transactor =
    val ds = SQLiteDataSource()
    ds.setUrl("jdbc:sqlite:" + sqliteDbPath)
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute("drop table if exists car")
      stmt.execute(
        """create table car (
            |    model text not null,
            |    id integer primary key,
            |    top_speed integer not null,
            |    vin integer,
            |    color text check (color in ('Red', 'Green', 'Blue')) not null,
            |    created text not null
            |)""".stripMargin
      )
      stmt.execute(
        """insert into car (model, id, top_speed, vin, color, created) values
            |('McLaren Senna', 1, 208, 123, 'Red', '2024-11-24T22:17:30.000000000Z'),
            |('Ferrari F8 Tributo', 2, 212, 124, 'Green', '2024-11-24T22:17:31.000000000Z'),
            |('Aston Martin Superleggera', 3, 211, null, 'Blue', '2024-11-24T22:17:32.000000000Z')""".stripMargin
      )
      stmt.execute("drop table if exists person")
      stmt.execute(
        """create table person (
            |    id integer primary key,
            |    first_name text,
            |    last_name text not null,
            |    is_admin integer not null,
            |    created text not null,
            |    social_id varchar(36)
            |)""".stripMargin
      )
      stmt.execute(
        """insert into person (id, first_name, last_name, is_admin, created, social_id) values
            |(1, 'George', 'Washington', true, '2024-11-24T22:17:30.000000000Z', 'd06443a6-3efb-46c4-a66a-a80a8a9a5388'),
            |(2, 'Alexander', 'Hamilton', true, '2024-11-24T22:17:30.000000000Z', '529b6c6d-7228-4da5-81d7-13b706f78ddb'),
            |(3, 'John', 'Adams', true, '2024-11-24T22:17:30.000000000Z', null),
            |(4, 'Benjamin', 'Franklin', true, '2024-11-24T22:17:30.000000000Z', null),
            |(5, 'John', 'Jay', true, '2024-11-24T22:17:30.000000000Z', null),
            |(6, 'Thomas', 'Jefferson', true, '2024-11-24T22:17:30.000000000Z', null),
            |(7, 'James', 'Madison', true, '2024-11-24T22:17:30.000000000Z', null),
            |(8, null, 'Nagro', false, '2024-11-24T22:17:30.000000000Z', null)""".stripMargin
      )
      stmt.execute("drop table if exists my_user")
      stmt.execute(
        """create table my_user (
          |  first_name text not null,
          |  id integer primary key
          |)""".stripMargin
      )
      stmt.execute(
        """insert into my_user (first_name) values
          |('George'),
          |('Alexander'),
          |('John')""".stripMargin
      )
      stmt.execute("drop table if exists no_id")
      stmt.execute(
        """create table no_id (
          |  created_at text not null,
          |  user_name text not null,
          |  user_action text not null
          |)""".stripMargin
      )
      stmt.execute(
        """insert into no_id values
          |('2024-11-24T22:17:30.000000000Z', 'Josh', 'clicked a button'),
          |('2024-11-24T22:17:30.000000000Z', 'Danny', 'opened a toaster'),
          |('2024-11-24T22:17:30.000000000Z', 'Greg', 'ran some QA tests');""".stripMargin
      )
      stmt.execute("drop table if exists big_dec")
      stmt.execute(
        """create table big_dec (
          |  id integer primary key,
          |  my_big_dec text
          |)""".stripMargin
      )
      stmt.execute(
        """insert into big_dec values
          |(1, '123'),
          |(2, null)""".stripMargin
      )
      stmt.execute("drop table if exists my_time")
      stmt.execute(
        """create table my_time (
          |  a text not null,
          |  b text not null,
          |  c text not null,
          |  d text not null
          |)""".stripMargin
      )
      stmt.execute(
        """insert into my_time values
          |('2025-03-30T21:19:23Z', '2025-03-30', '05:20:04', '2025-04-02T20:16:38'),
          |('2025-03-31T21:19:23Z', '2025-03-31', '05:30:04', '2025-04-02T20:17:38')""".stripMargin
      )
    ).get
    Transactor(ds)
  end xa
end SqliteTests
