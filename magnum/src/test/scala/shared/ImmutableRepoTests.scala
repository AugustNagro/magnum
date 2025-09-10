package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.{OffsetDateTime, ZoneOffset}
import scala.util.Using

def immutableRepoTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using
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

  val carRepo = ImmutableRepo[Car, Long]
  val car = TableInfo[Car, Car, Long]

  val allCars = Vector(
    Car(
      model = "McLaren Senna",
      id = 1L,
      topSpeed = 208,
      vinNumber = Some(123),
      color = Color.Red,
      created = OffsetDateTime.parse("2024-11-24T22:17:30.000000000Z")
    ),
    Car(
      model = "Ferrari F8 Tributo",
      id = 2L,
      topSpeed = 212,
      vinNumber = Some(124),
      color = Color.Green,
      created = OffsetDateTime.parse("2024-11-24T22:17:31.000000000Z")
    ),
    Car(
      model = "Aston Martin Superleggera",
      id = 3L,
      topSpeed = 211,
      vinNumber = None,
      color = Color.Blue,
      created = OffsetDateTime.parse("2024-11-24T22:17:32.000000000Z")
    )
  )

  test("count"):
    xa().connect:
      assert(carRepo.count == 3L)

  test("existsById"):
    xa().connect:
      assert(carRepo.existsById(3L))
      assert(!carRepo.existsById(4L))

  test("findAll"):
    val cars = xa().connect:
      carRepo.findAll
    assert(cars == allCars)

  test("findById"):
    xa().connect:
      assert(carRepo.findById(3L).get == allCars.last)
      assert(carRepo.findById(4L) == None)

  test("findAllByIds"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val ids = carRepo.findAllById(Vector(1L, 3L)).map(_.id)
      assert(ids == Vector(1L, 3L))

  test("serializable transaction"):
    xa()
      .withConnectionConfig(withSerializable)
      .transact:
        assert(carRepo.count == 3L)

  def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

  test("select query"):
    xa().connect:
      val minSpeed: Int = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
          .query[Car]
      assertNoDiff(
        query.frag.sqlString,
        "select model, id, top_speed, vin, color, created from car where top_speed > ?"
      )
      assert(query.frag.params == Vector(minSpeed))
      assert(query.run() == allCars.tail)

  test("select query with aliasing"):
    xa().connect:
      val minSpeed = 210
      val cAlias = car.alias("c")
      val query =
        sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
          .query[Car]
      assertNoDiff(
        query.frag.sqlString,
        "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car AS c where c.top_speed > ?"
      )
      assert(query.frag.params == Vector(minSpeed))
      assert(query.run() == allCars.tail)

  test("select query with aliasing and joins"):
    xa().connect:
      val c1 = car.alias("c1")
      val c2 = car.alias("c2")
      val query =
        sql"SELECT ${c1.model}, ${c2.model} FROM $c1 JOIN $c2 ON ${c1.topSpeed} > ${c2.topSpeed}"
          .query[(String, String)]
      assertNoDiff(
        query.frag.sqlString,
        "SELECT c1.model, c2.model FROM car AS c1 JOIN car AS c2 ON c1.top_speed > c2.top_speed"
      )
      assert(query.run().nonEmpty)

  test("select via option"):
    xa().connect:
      val vin = Option(124)
      val cars =
        sql"select * from car where vin = $vin"
          .query[Car]
          .run()
      assert(cars == allCars.filter(_.vinNumber == vin))

  test("tuple select"):
    xa().connect:
      val tuples = sql"select model, color from car where id = 2"
        .query[(String, Color)]
        .run()
      assert(tuples == Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    xa().connect:
      assert(carRepo.findById(3L).get.vinNumber == None)

  test("created timestamps should match"):
    xa().connect:
      assert(carRepo.findAll.map(_.created) == allCars.map(_.created))

  test(".query iterator"):
    xa().connect:
      Using.Manager(implicit use =>
        val it = sql"SELECT * FROM car".query[Car].iterator()
        assert(it.map(_.id).size == 3)
      )

  test("sql interpolator selects right DbCodec"):
    case class Coord(x: Double, y: Double)

    given DbCodec[Coord] with
      def cols: IArray[Int] = IArray(java.sql.Types.BINARY)
      def queryRepr: String = "MyCoord(?)"
      def readSingle(rs: ResultSet, pos: Int): Coord = ???
      def readSingleOption(rs: ResultSet, pos: Int): Option[Coord] = ???
      def writeSingle(coord: Coord, ps: PreparedStatement, pos: Int): Unit = ???

    val myCoord = Coord(1, 2)

    val query = sql"SELECT * FROM test WHERE coord = $myCoord"

    assert(query.sqlString.contains("MyCoord(?)"))

end immutableRepoTests
