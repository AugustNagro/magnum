package com.augustnagro.magnum.magzio

import munit.FunSuite
import shared.Color
import zio.*

import java.sql.Connection
import java.time.OffsetDateTime
import scala.util.{Success, Using}

def immutableRepoZioTests(
    suite: FunSuite,
    dbType: DbType,
    xa: UIO[Transactor]
)(using
    munit.Location,
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  val runtime: Runtime[Any] = zio.Runtime.default

  def runIO[A](io: ZIO[Any, Throwable, A]): A =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(io).getOrThrow()
    }

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

//  test("error handling"):
//    enum Errors:
//      case RateLimit
//      case ExistingColor
//
//    runIO:
//      magzio.transact(xa()):
//        val now = OffsetDateTime.parse("2024-11-24T22:17:33.000000000Z")
//        val latestCar = sql"SELECT ${car.all} FROM $car ORDER BY created DESC"
//          .query[Car]
//          .run()
//          .headOption
//        if latestCar.exists(_.created.isAfter(now.minusSeconds(4))) then

  test("count"):
    val count = runIO(
      xa.flatMap:
        _.connectUIO:
          carRepo.count
    )
    assert(count == 3)

  /*
  test("existsById"):
    val (exists3, exists4) =
      xa().connectUIO:
        carRepo.existsById(3L) -> carRepo.existsById(4L)
    assert(exists3)
    assert(!exists4)

  test("findAll"):
    val cars =
      xa().connectUIO:
        carRepo.findAll
    assert(cars == allCars)

  test("findById"):
    val (exists3, exists4) =
      xa().connectUIO:
        carRepo.findById(3L) -> carRepo.findById(4L)
    assert(exists3.get == allCars.last)
    assert(exists4 == None)

  test("findAllByIds"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    val ids =
      xa().connectUIO:
        carRepo.findAllById(Vector(1L, 3L)).map(_.id)
    assert(ids == Vector(1L, 3L))

  test("serializable transaction"):
    val count =
      magzio.transact(xa().copy(connectionConfig = withSerializable)):
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
        xa().connectUIO:
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
        xa().connectUIO:
          query.run()
    assertNoDiff(
      query.frag.sqlString,
      "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?"
    )
    assert(query.frag.params == Vector(minSpeed))
    assert(result == allCars.tail)

  test("select via option"):
    val vin = Some(124)
    val cars =
      xa().connectUIO:
        sql"select * from car where vin = $vin"
          .query[Car]
          .run()
    assert(cars == allCars.filter(_.vinNumber == vin))

  test("tuple select"):
    val tuples =
      xa().connectUIO:
        sql"select model, color from car where id = 2"
          .query[(String, Color)]
          .run()
    assert(tuples == Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    val maybeCar =
      xa().connectUIO:
        carRepo.findById(3L)
    assert(maybeCar.get.vinNumber == None)

  test("created timestamps should match"):
    val allCars =
      xa().connectUIO:
        carRepo.findAll
    assert(allCars.map(_.created) == allCars.map(_.created))

  test(".query iterator"):
    val carsCount =
      xa().connectUIO:
        Using.Manager(implicit use =>
          val it = sql"SELECT * FROM car".query[Car].iterator()
          it.map(_.id).size
        )
    assert(carsCount == Success(3))

   */

end immutableRepoZioTests
