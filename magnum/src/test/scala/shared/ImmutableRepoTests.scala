package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.{
  ClickhouseDbType,
  DbType,
  MySqlDbType,
  OracleDbType,
  SqliteDbType
}
import com.augustnagro.magnum.repo.{
  Id,
  ImmutableRepo,
  SqlName,
  SqlNameMapper,
  Table
}
import com.augustnagro.magnum.shared.builders.Transactor
import com.augustnagro.magnum.tableinfo.TableInfo
import munit.FunSuite

import java.sql.Connection
import java.time.{OffsetDateTime, ZoneOffset}
import scala.util.Using

def immutableRepoTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using
    munit.Location,
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
    connect(xa()):
      assert(carRepo.count == 3L)

  test("existsById"):
    connect(xa()):
      assert(carRepo.existsById(3L))
      assert(!carRepo.existsById(4L))

  test("findAll"):
    val cars = connect(xa()):
      carRepo.findAll
    assert(cars == allCars)

  test("findById"):
    connect(xa()):
      assert(carRepo.findById(3L).get == allCars.last)
      assert(carRepo.findById(4L) == None)

  test("findAllByIds"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    connect(xa()):
      val ids = carRepo.findAllById(Vector(1L, 3L)).map(_.id)
      assert(ids == Vector(1L, 3L))

  test("serializable transaction"):
    transact(xa().copy(connectionConfig = withSerializable)):
      assert(carRepo.count == 3L)

  def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

  test("select query"):
    connect(xa()):
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
    connect(xa()):
      val minSpeed = 210
      val cAlias = car.alias("c")
      val query =
        sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
          .query[Car]
      assertNoDiff(
        query.frag.sqlString,
        "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?"
      )
      assert(query.frag.params == Vector(minSpeed))
      assert(query.run() == allCars.tail)

  test("select via option"):
    connect(xa()):
      val vin = Some(124)
      val cars =
        sql"select * from car where vin = $vin"
          .query[Car]
          .run()
      assert(cars == allCars.filter(_.vinNumber == vin))

  test("tuple select"):
    connect(xa()):
      val tuples = sql"select model, color from car where id = 2"
        .query[(String, Color)]
        .run()
      assert(tuples == Vector(allCars(1).model -> allCars(1).color))

  test("reads null int as None and not Some(0)"):
    connect(xa()):
      assert(carRepo.findById(3L).get.vinNumber == None)

  test("created timestamps should match"):
    connect(xa()):
      assert(carRepo.findAll.map(_.created) == allCars.map(_.created))

  test(".query iterator"):
    connect(xa()):
      Using.Manager(implicit use =>
        val it = sql"SELECT * FROM car".query[Car].iterator()
        assert(it.map(_.id).size == 3)
      )
end immutableRepoTests
