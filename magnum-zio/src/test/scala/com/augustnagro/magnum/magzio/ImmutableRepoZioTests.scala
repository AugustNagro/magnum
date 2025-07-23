package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}
import zio.*

import java.sql.Connection
import java.time.OffsetDateTime
import scala.util.{Success, Using}

def immutableRepoZioTests(
    suite: FunSuite,
    dbType: DbType,
    xa: () => TransactorZIO
)(using
    Location,
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  val runtime: Runtime[Any] = zio.Runtime.default

  def runIO[A](io: ZIO[Any, Throwable, A]): A =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(io).getOrThrow()
    }

  enum Color derives DbCodec:
    case Red, Green, Blue

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
    val count =
      runIO:
        xa().connect:
          carRepo.count
    assert(count == 3L)

  test("existsById"):
    val (exists3, exists4) =
      runIO:
        xa().connect:
          carRepo.existsById(3L) -> carRepo.existsById(4L)
    assert(exists3)
    assert(!exists4)

  test("findAll"):
    val cars =
      runIO:
        xa().connect:
          carRepo.findAll
    assert(cars == allCars)

  test("findById"):
    val (exists3, exists4) =
      runIO:
        xa().connect:
          carRepo.findById(3L) -> carRepo.findById(4L)
    assert(exists3.get == allCars.last)
    assert(exists4 == None)

  test("findAllByIds"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    val ids =
      runIO:
        xa().connect:
          carRepo.findAllById(Vector(1L, 3L)).map(_.id)
    assert(ids == Vector(1L, 3L))

  test("serializable transaction"):
    val count =
      runIO:
        xa()
          .withConnectionConfig(withSerializable)
          .transact:
            carRepo.count
    assert(count == 3L)

  def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

  test("select query"):
    val minSpeed: Int = 210
    val query =
      sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
        .query[Car]
    val result =
      runIO:
        xa().connect:
          query.run()
    assertNoDiff(
      query.frag.sqlString,
      "select model, id, top_speed, vin, color, created from car where top_speed > ?"
    )
    assert(query.frag.params == Vector(minSpeed))
    assert(result == allCars.tail)

  test("select query with aliasing"):
    val minSpeed = 210
    val cAlias = car.alias("c")
    val query =
      sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
        .query[Car]
    val result =
      runIO:
        xa().connect:
          query.run()
    assertNoDiff(
      query.frag.sqlString,
      "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?"
    )
    assert(query.frag.params == Vector(minSpeed))
    assert(result == allCars.tail)

  test("select via option"):
    val vin = Option(124)
    val cars =
      runIO:
        xa().connect:
          sql"select * from car where vin = $vin"
            .query[Car]
            .run()
    assert(cars == allCars.filter(_.vinNumber == vin))

  test("tuple select"):
    val tuples =
      runIO:
        xa().connect:
          sql"select model, color from car where id = 2"
            .query[(String, Color)]
            .run()
    assert(tuples == Vector(allCars(1).model -> allCars(1).color))
    
  test("large tuple select"):
    val tuples =
      runIO:
        xa().connect:
          sql"select model, color, top_speed, id, vin from car where id = 2"
            .query[(String, Color, Int, Long, Option[Int])]
            .run()
    assert(tuples == Vector(
      allCars(1).model -> allCars(1).color -> allCars(1).topSpeed -> allCars(1).id -> allCars(1).vinNumber
    ))

  test("reads null int as None and not Some(0)"):
    val maybeCar =
      runIO:
        xa().connect:
          carRepo.findById(3L)
    assert(maybeCar.get.vinNumber == None)

  test("created timestamps should match"):
    val allCars =
      runIO:
        xa().connect:
          carRepo.findAll
    assert(allCars.map(_.created) == allCars.map(_.created))

  test(".query iterator"):
    val carsCount =
      runIO:
        xa().connect:
          Using.Manager(implicit use =>
            val it = sql"SELECT * FROM car".query[Car].iterator()
            it.map(_.id).size
          )
    assert(carsCount == Success(3))

end immutableRepoZioTests
