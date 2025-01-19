package com.augustnagro.magnum.magzio

import zio.test.*
import zio.*
import com.augustnagro.magnum
import com.augustnagro.magnum.TransactorOps
import java.time.OffsetDateTime
import com.augustnagro.magnum.containers.DataSourceProvider
import scala.reflect.ClassTag
import com.augustnagro.magnum.containers.Postgres
import scala.util.Using

object GenericSpec extends ZIOSpecDefault:
  trait Converter[T[_]: TagK]:
    def convert[A: Tag](t: T[A]): Task[A]
  object Converter:
    val plain = ZLayer.succeed {
      new Converter[magnum.Plain]:
        def convert[A: Tag](t: magnum.Plain[A]): Task[A] = ZIO.succeed(t: A)
    }
    val identity = ZLayer.succeed {
      new Converter[Task]:
        def convert[A: Tag](t: Task[A]): Task[A] = t
    }

  def specs[T[_]: TagK](name: String, dbType: DbType) =
    def xa[A: Tag](
        f: TransactorOps[T] => T[A]
    )(using ClassTag[A]) =
      for
        converter <- ZIO.service[Converter[T]]
        t <- ZIO.service[TransactorOps[T]]
        res <- converter.convert(f(t))
      yield (res)

    val all = suiteAll(
      s"$name transactor Specs for ${dbType.getClass().getSimpleName().replaceAll("\\$", "")}"
    ):
      enum Color derives magnum.DbCodec:
        case Red, Green, Blue

      @Table(dbType, SqlNameMapper.CamelToSnakeCase)
      case class Car(
          model: String,
          @Id id: Long,
          topSpeed: Int,
          @SqlName("vin") vinNumber: Option[Int],
          color: Color,
          created: OffsetDateTime
      ) derives magnum.DbCodec

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
        /** assume(dbType != ClickhouseDbType) assume(dbType != MySqlDbType)
          * assume(dbType != OracleDbType) assume(dbType != SqliteDbType)
          */
        for cars <-
            xa:
              _.connect(carRepo.findAllById(List(1L, 2L)))
        yield assertTrue(cars == List(allCars(0), allCars(1)))
      @@ TestAspect.ifEnvNotSet("NO_FIND_ALL_BY_IDS")

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

    all
  end specs
  def spec = suiteAll("all specs"):
    specs[Task]("zio", PostgresDbType).provideShared(
      DataSourceProvider.defaultPostgresTransactor,
      Converter.identity
    )
    specs[magnum.Plain]("plain", PostgresDbType).provideShared(
      Postgres.default >>> DataSourceProvider.datasource.flatMap(ds =>
        ZLayer.succeed(magnum.Transactor(ds.get))
      ),
      Converter.plain
    )
  @@ TestAspect.sequential
end GenericSpec
