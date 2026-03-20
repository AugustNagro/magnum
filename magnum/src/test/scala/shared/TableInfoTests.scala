package shared

import com.augustnagro.magnum.*
import munit.FunSuite

import java.time.OffsetDateTime
import java.util.UUID

def tableInfoTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    munit.Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime,
      socialId: Option[UUID]
  ) derives DbCodec

  val person = TableInfo[Person, Person, Long]

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Car(
      model: String,
      @Id id: Long,
      topSpeed: Int,
      @SqlName("vin") vinNumber: Option[Int],
      color: Color,
      created: OffsetDateTime
  ) derives DbCodec

  val car = TableInfo[Car, Car, Long]

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class NoId(
      createdAt: OffsetDateTime,
      userName: String,
      userAction: String
  ) derives DbCodec

  val noIdTableInfo = TableInfo[NoId, NoId, Null]

  test("NoId TableInfo.idColumn == None"):
    assert(noIdTableInfo.idColumn == None)

  test("can use TableInfo.idColumn to scrap boilerplate"):
    extension [EC: DbCodec, E, ID](table: TableInfo[EC, E, ID])
      def onConflictDoUpdate(entityCreator: EC): Update =
        val updatedCols = table.all.columnNames
          .filterNot(col => table.idColumn.exists(_.scalaName == col.scalaName))
          .map(col => sql"$col = EXCLUDED.$col")
          .reduceLeft((a, b) => sql"$a, $b")
        sql"""INSERT INTO $table ${table.insertColumns} VALUES ($entityCreator)
             |ON CONFLICT DO UPDATE SET $updatedCols
             |""".stripMargin.update

    assertEquals(
      person
        .onConflictDoUpdate(
          Person(1, None, "Smith", false, OffsetDateTime.now, None)
        )
        .frag
        .sqlString,
      """INSERT INTO person (id, first_name, last_name, is_admin, created, social_id) VALUES (?, ?, ?, ?, ?, ?)
        |ON CONFLICT DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, is_admin = EXCLUDED.is_admin, created = EXCLUDED.created, social_id = EXCLUDED.social_id
        |""".stripMargin
    )

    assertEquals(
      car
        .onConflictDoUpdate(
          Car("Tesla", 2L, 123, None, Color.Red, OffsetDateTime.now)
        )
        .frag
        .sqlString,
      """INSERT INTO car (model, id, top_speed, vin, color, created) VALUES (?, ?, ?, ?, ?, ?)
        |ON CONFLICT DO UPDATE SET model = EXCLUDED.model, top_speed = EXCLUDED.top_speed, vin = EXCLUDED.vin, color = EXCLUDED.color, created = EXCLUDED.created
        |""".stripMargin
    )

    assertEquals(
      noIdTableInfo
        .onConflictDoUpdate(NoId(OffsetDateTime.now, "abc", "def"))
        .frag
        .sqlString,
      """INSERT INTO no_id (created_at, user_name, user_action) VALUES (?, ?, ?)
        |ON CONFLICT DO UPDATE SET created_at = EXCLUDED.created_at, user_name = EXCLUDED.user_name, user_action = EXCLUDED.user_action
        |""".stripMargin
    )
end tableInfoTests
