package com.augustnagro.magnum.magzio

import zio.test.*
import zio.*
import com.augustnagro.magnum.containers.DataSourceProvider.defaultTransactor
import com.augustnagro.magnum.magzio.*
import java.time.OffsetDateTime
import java.sql.Connection
import scala.util.Using

object ImmutableRepoSpecs extends ZIOSpecDefault:
  def xa[A](
      f: Transactor => ZIO[Any, Throwable, A]
  ): ZIO[Transactor, Throwable, A] =
    ZIO.serviceWithZIO[Transactor](f)
  val specs = suiteAll("Magnum ZIO Specs"):
    enum Color derives DbCodec:
      case Red, Green, Blue

    @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
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
      for count <-
          xa:
            _.connect(carRepo.count)
      yield assertTrue(count == 3)

    test("exists by id"):
      for
        exists3 <-
          xa:
            _.connect(carRepo.existsById(3L))
        exists4 <-
          xa:
            _.connect(carRepo.existsById(4L))
      yield assertTrue(exists3) && assertTrue(!exists4)

    test("find all"):
      for cars <-
          xa:
            _.connect(carRepo.findAll)
      yield assertTrue(cars == allCars)

    test("find by id"):
      for
        car <-
          xa:
            _.connect(carRepo.findById(1L))
        missing <-
          xa:
            _.connect(carRepo.findById(4L))
      yield assertTrue(car == Some(allCars(0))) && assertTrue(missing == None)

    test("find all by ids"):
      for cars <-
          xa:
            _.connect(carRepo.findAllById(List(1L, 2L)))
      yield assertTrue(cars == List(allCars(0), allCars(1)))

    test("serializable transaction"):
      def withSerializable(con: Connection): Unit =
        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
      for count <- xa: x =>
          x.withConnectionConfig(withSerializable).transact(carRepo.count)
      yield assertTrue(count == 3)

    test("select query"):
      val minSpeed: Int = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
          .query[Car]
      for cars <-
          xa:
            _.connect(query.run())
      yield assertTrue(cars == allCars.tail) &&
        assertTrue(
          query.frag.sqlString == "select model, id, top_speed, vin, color, created from car where top_speed > ?"
        ) &&
        assertTrue(query.frag.params == Vector(minSpeed))

    test("select query with aliasing"):
      val minSpeed: Int = 210
      val cAlias = car.alias("c")
      val query =
        sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
          .query[Car]
      for cars <-
          xa:
            _.connect(query.run())
      yield assertTrue(cars == allCars.tail) && assertTrue(
        query.frag.sqlString == "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?"
      ) && assertTrue(query.frag.params == Vector(minSpeed))

    test("select via option"):
      val vin = Some(124)
      for cars <-
          xa:
            _.connect(
              sql"select * from car where vin = $vin".query[Car].run()
            )
      yield assertTrue(cars == allCars.filter(_.vinNumber == vin))

    test("tuple select"):
      for tuples <-
          xa:
            _.connect(
              sql"select ${car.id}, ${car.model} from $car"
                .query[(Long, String)]
                .run()
            )
      yield assertTrue(tuples == allCars.map(c => (c.id, c.model)))

    test("reads null in as None and not Some(0)"):
      for car <-
          xa:
            _.connect(
              carRepo.findById(3L)
            )
      yield assertTrue(car.exists(_.vinNumber.isEmpty))

    test("created timestamps should match"):
      for
        cars1 <-
          xa:
            _.connect(
              sql"select * from car".query[Car].run()
            )
        cars2 <-
          xa:
            _.connect(
              carRepo.findAll
            )
      yield assertTrue(cars1.map(_.created) == allCars.map(_.created)) &&
        assertTrue(cars2.map(_.created) == allCars.map(_.created)) &&
        assertTrue(cars1 == cars2)

    test(".query iterator"):
      for
        t <-
          xa:
            _.connect:
              Using.Manager:
                implicit use =>
                  sql"select * from car".query[Car].iterator().map(_.id).size
        size <- ZIO.fromTry(t)
      yield assertTrue(size == 3)
  def spec = specs.provideShared(defaultTransactor)
end ImmutableRepoSpecs
