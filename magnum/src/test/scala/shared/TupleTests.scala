package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.sql.{PreparedStatement, ResultSet}
import java.time.OffsetDateTime

def tupleTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    Location,
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

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

  val car1 = Car(
    model = "Ferrari F8 Tributo",
    id = 2L,
    topSpeed = 212,
    vinNumber = Some(124),
    color = Color.Green,
    created = OffsetDateTime.parse("2024-11-24T22:17:31.000000000Z")
  )

  test("large tuple support does not override hand-rolled Tuple[2-4] codecs"):
    val tuple2ACodec = summon[DbCodec[(String, Color)]]
    val tuple2BCodec = summon[DbCodec[(String, Int)]]
    assert(tuple2ACodec.getClass == tuple2BCodec.getClass)
    val tuple5ACodec = summon[DbCodec[(String, Color, Int, Long, Option[Int])]]
    assert(tuple5ACodec.getClass != tuple2ACodec.getClass)
    val tuple5BCodec = summon[DbCodec[(Int, Int, Int, Long, Option[Int])]]
    assert(tuple5BCodec.getClass != tuple5ACodec.getClass)

  test("large tuple select"):
    val tuple = xa().connect:
      sql"select model, color, top_speed, id, vin from car where id = 2"
        .query[(String, Color, Int, Long, Option[Int])]
        .run()
        .head
    assert(
      tuple == (car1.model, car1.color, car1.topSpeed, car1.id, car1.vinNumber)
    )

  test("large tuple select option"):
    assume(dbType != OracleDbType)
    val tupleA = xa().connect:
      sql"select model, color, top_speed, id, vin from car where id = 1"
        .query[Option[(String, Color, Int, Long, Option[Int])]]
        .run()
        .head
    assert(tupleA.isDefined)
    val someTuple = xa().connect:
      sql"select 1, 1, 1, 1, 1, 1"
        .query[Option[(Int, Int, Int, Int, Int, Int)]]
        .run()
        .head
    assert(someTuple.isDefined)
    val noneTuple = xa().connect:
      sql"select 1, 1, 1, 1, null, 1"
        .query[Option[(Int, Int, Int, Int, Int, Int)]]
        .run()
        .head
    assert(noneTuple.isEmpty)
    val optionTupleOption = xa().connect:
      sql"select 1, 1, 1, 1, null, 1"
        .query[Option[(Int, Int, Int, Int, Option[Int], Int)]]
        .run()
        .head
    assert(optionTupleOption.isDefined)

  test("large tuple write"):
    val tup =
      ("Ford Edge", 4L, 101, Option(12345), Color.Red, OffsetDateTime.now)
    val tupWithSome =
      ("Ford Edge", 5L, 101, Some(12345), Color.Red, OffsetDateTime.now)
    xa().connect:
      sql"insert into $car ${car.insertColumns} values $tup".update.run()
      sql"insert into $car ${car.insertColumns} values $tupWithSome".update
        .run()
      val res =
        sql"select * from $car where ${car.id} = 4".query[Car].run().head
      assert(res.color == Color.Red)

end tupleTests
