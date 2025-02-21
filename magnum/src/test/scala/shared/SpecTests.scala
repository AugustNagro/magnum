package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.time.{OffsetDateTime, ZoneOffset}

opaque type CarId = Long
object CarId:
  def apply(value: Long): CarId = value
  extension (opaque: CarId) def value: Long = opaque
  given DbCodec[CarId] =
    DbCodec.LongCodec.biMap(CarId.apply, _.value)

def specTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
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

  test("select all"):
    xa().transact:
      val spec = Spec[Car]
      assert(carRepo.findAll(spec) == allCars)

  test("empty predicate"):
    xa().transact:
      val spec = Spec[Car].where(sql"")
      assert(carRepo.findAll(spec) == allCars)

  test("predicate having param at end"):
    xa().transact:
      val id = CarId(2L)
      val spec = Spec[Car].where(sql"$id < id")
      assert(carRepo.findAll(spec) == Vector(allCars.last))

  test("AND in where predicate"):
    xa().transact:
      val color = Color.Red
      val model = "MCLAREN SENNA"
      val spec =
        Spec[Car].where(sql"color = $color AND $model = upper(model)")
      assert(carRepo.findAll(spec) == Vector(allCars.head))

  test("multiple where parameters"):
    xa().transact:
      val color = Color.Red
      val model = "MCLAREN SENNA"
      val spec = Spec[Car]
        .where(sql"color = $color")
        .where(sql"$model = upper(model)")
      assert(carRepo.findAll(spec) == Vector(allCars.head))

  test("orderBy"):
    xa().transact:
      val spec = Spec[Car].orderBy("top_speed")
      assert(carRepo.findAll(spec) == allCars.sortBy(_.topSpeed))

  test("orderBy null with sort order and null order"):
    xa().transact:
      val spec = Spec[Car]
        .orderBy("vin", SortOrder.Desc, NullOrder.First)
      assert(carRepo.findAll(spec) == allCars.reverse)

  test("limit"):
    xa().transact:
      val spec = Spec[Car].limit(2)
      assert(carRepo.findAll(spec).size == 2)

  test("offset"):
    xa().transact:
      val spec = Spec[Car].offset(1)
      assert(carRepo.findAll(spec) == allCars.tail)

  test("seek"):
    xa().transact:
      val spec = Spec[Car].seek("id", SeekDir.Gt, 2, SortOrder.Asc)
      assert(carRepo.findAll(spec).size == 1)

  test("seek multiple"):
    xa().transact:
      val spec = Spec[Car]
        .seek("id", SeekDir.Lt, 3, SortOrder.Asc)
        .seek("top_speed", SeekDir.Gt, 210, SortOrder.Asc)
      assert(carRepo.findAll(spec) == Vector(allCars(1)))

  test("everything"):
    xa().transact:
      val idOpt = Option.empty[CarId]
      val speed = 210
      val spec = Spec[Car]
        .where(idOpt.map(id => sql"id = $id").getOrElse(sql""))
        .where(sql"top_speed > $speed")
        .orderBy("model", SortOrder.Desc)
        .limit(1)
        .seek("vin", SeekDir.Gt, 1, SortOrder.Asc, NullOrder.Last)
      assert(carRepo.findAll(spec) == Vector(allCars(1)))

  test("prefix"):
    xa().transact:
      val c = car.alias("c")
      val color = Color.Red
      val spec = Spec[Car]
        .prefix(sql"SELECT ${c.all} FROM $c")
        .where(sql"${c.color} = $color")
      assert(carRepo.findAll(spec) == Vector(allCars.head))

  test("prefix with embedded sql"):
    xa().transact:
      val c = car.alias("c")
      val color = Color.Red
      val selectPart = sql"SELECT ${c.all}"
      val fromPart = sql"FROM $c"
      val spec = Spec[Car]
        .prefix(sql"$selectPart $fromPart")
        .where(sql"${c.color} = $color")
      assert(carRepo.findAll(spec) == Vector(allCars.head))
end specTests
