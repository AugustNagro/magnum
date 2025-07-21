package com.augustnagro.magnum.magcats

import munit.FunSuite
import cats.effect.IO
import cats.syntax.monadError.*
import cats.effect.std.Dispatcher
import cats.effect.kernel.Outcome.*
import munit.CatsEffectAssertions.*

import java.sql.Connection
import java.time.OffsetDateTime
import scala.util.{Success, Using}
import munit.catseffect.IOFixture

import com.augustnagro.magnum.pg.enums.PgEnumDbCodec
import java.time.Duration
import cats.effect.kernel.Outcome
import scala.concurrent.duration.*
import java.util.concurrent.TimeUnit

def repoCatsEffectTests(
    suite: FunSuite,
    dbType: DbType,
    xa: () => Transactor[IO]
)(using
    munit.Location,
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

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

  val carRepo = Repo[Car, Car, Long]
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

  val carToInsert = Car(
    model = "Lamborghini Huracán",
    id = 7L,
    topSpeed = 202,
    vinNumber = Some(9876),
    color = Color.Red,
    created = OffsetDateTime.parse("2023-08-15T14:45:22.000000000Z")
  )

  test("count"):
    val count =
      xa().connect:
        carRepo.count
    assertIO(count, 3L)

  test("existsById"):
    val exists3And4 =
      xa().connect:
        carRepo.existsById(3L) -> carRepo.existsById(4L)
    assertIO(exists3And4, true -> false)

  test("findAll"):
    val cars =
      xa().connect:
        carRepo.findAll
    assertIO(cars, allCars)

  test("findById"):
    val exists3And4 =
      xa().connect:
        carRepo.findById(3L) -> carRepo.findById(4L)
    assertIO(exists3And4, Some(allCars.last) -> None)

  test("findAllByIds"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    val ids =
      xa().connect:
        carRepo.findAllById(Vector(1L, 3L)).map(_.id)
    assertIO(ids, Vector(1L, 3L))

  test("serializable transaction"):
    val count =
      xa()
        .withConnectionConfig(withSerializable)
        .transact:
          carRepo.count
    assertIO(count, 3L)

  def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

  test("select query"):
    val minSpeed: Int = 210
    val query =
      sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
        .query[Car]
    val result =
      xa().connect:
        query.run()
    assertNoDiff(
      query.frag.sqlString,
      "select model, id, top_speed, vin, color, created from car where top_speed > ?"
    )
    assert(query.frag.params == Vector(minSpeed))
    assertIO(result, allCars.tail)

  test("select query with aliasing"):
    val minSpeed = 210
    val cAlias = car.alias("c")
    val query =
      sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
        .query[Car]
    val result =
      xa().connect:
        query.run()
    assertNoDiff(
      query.frag.sqlString,
      "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?"
    )
    assert(query.frag.params == Vector(minSpeed))
    assertIO(result, allCars.tail)

  test("select via option"):
    val vin: Option[Int] = Some(124)
    val cars =
      xa().connect:
        sql"select * from car where vin = $vin"
          .query[Car]
          .run()
    assertIO(cars, allCars.filter(_.vinNumber == vin))

  test("tuple select"):
    val tuples =
      xa().connect:
        sql"select model, color from car where id = 2"
          .query[(String, Color)]
          .run()
    assertIO(tuples, Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    val maybeCar =
      xa().connect:
        carRepo.findById(3L)
    assertIO(maybeCar.map(_.get.vinNumber), None)

  test("created timestamps should match"):
    val allCarsIO =
      xa().connect:
        carRepo.findAll
    assertIO(allCarsIO.map(_.map(_.created)), allCars.map(_.created))

  test(".query iterator"):
    val carsCount =
      xa().connect:
        Using.Manager(implicit use =>
          val it = sql"SELECT * FROM car".query[Car].iterator()
          it.map(_.id).size
        )
    assertIO(carsCount, Success(3))

  test("insert with enum"):
    val transactor = xa()
    val count =
      transactor.connect:
        carRepo.insert(carToInsert)
        carRepo.count

    assertIO(count, 4L)

  test("long running query"):
    val transactor = xa()
    val firstIo =
      transactor.transact:
        carRepo.insert(carToInsert)

    val countIO =
      transactor.connect(carRepo.count)

    for {
      startTime <- IO.realTimeInstant
      fiber <- firstIo.start
      _ <- fiber.cancel
      result <- fiber.join
      count <- countIO
      endTime <- IO.realTimeInstant
      elapsed = Duration.between(startTime, endTime).toMillis
    } yield {
      assert(result.isCanceled, s"Expected operation to be cancelled, got ${result.getClass().getName()}")
      assertEquals(count, 3L)
      assert(
        elapsed < 5000,
        s"Query should be cancelled quickly, but took $elapsed ms"
      )
    }
end repoCatsEffectTests
