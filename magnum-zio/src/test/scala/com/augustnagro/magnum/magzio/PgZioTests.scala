package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.*
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import zio.{test as _, *}
import zio.test.*

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.time.OffsetDateTime
import javax.sql.DataSource
import scala.util.Using
import scala.util.Using.Manager

object PgZioTests extends ZIOSpecDefault:

  override def spec =
    suite("PostgreSQL") {
      ZIO.serviceWith[PgZioTests](spec => Chunk(spec.tests))
    }.provideShared(
      transactorLayer >>> layer
    ) @@ TestAspect.sequential

  def layer: URLayer[TransactorZIO, PgZioTests] =
    ZLayer.fromFunction(PgZioTests.apply)

  def transactorLayer = dataSourceLayer >>> TransactorZIO.layer

  def dataSourceLayer: TaskLayer[DataSource] =
    ZLayer.scoped:
      ZIO
        .acquireRelease(
          ZIO.attemptBlocking:
            val postgres = new PostgreSQLContainer("postgres:18.6")
            postgres.start()
            postgres
        )(postgres => ZIO.attemptBlocking(postgres.stop()).orDie)
        .flatMap: postgres =>
          val dataSource = PGSimpleDataSource()
          dataSource.setUrl(postgres.getJdbcUrl())
          dataSource.setUser(postgres.getUsername())
          dataSource.setPassword(postgres.getPassword())
          ZIO.attemptBlocking(initializeSchema(dataSource)).as(dataSource)

  def initializeSchema(dataSource: PGSimpleDataSource): Unit =
    val tableDDLs = Vector(
      "/pg/car.sql",
      "/pg/person.sql",
      "/pg/my-user.sql",
      "/pg/no-id.sql",
      "/pg/big-dec.sql"
    ).map(path =>
      Using.resource(getClass.getResourceAsStream(path))(stream =>
        String(stream.readAllBytes(), StandardCharsets.UTF_8)
      )
    )

    Manager(use =>
      val connection = use(dataSource.getConnection())
      val statement = use(connection.createStatement())
      tableDDLs.foreach(ddl => statement.execute(ddl))
    ).get

end PgZioTests

class PgZioTests(xa: TransactorZIO):
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

  def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

  def tests = suite("immutable repository")(
    test("count") {
      for count <- xa.connect(carRepo.count)
      yield assertTrue(count == 3L)
    },
    test("existsById") {
      for (exists3, exists4) <-
          xa.connect((carRepo.existsById(3L), carRepo.existsById(4L)))
      yield assertTrue(exists3, !exists4)
    },
    test("findAll") {
      for cars <- xa.connect(carRepo.findAll)
      yield assertTrue(cars == allCars)
    },
    test("findById") {
      for (car3, car4) <-
          xa.connect((carRepo.findById(3L), carRepo.findById(4L)))
      yield assertTrue(car3.contains(allCars.last), car4.isEmpty)
    },
    test("findAllByIds") {
      for ids <- xa.connect(carRepo.findAllById(Vector(1L, 3L)).map(_.id))
      yield assertTrue(ids == Vector(1L, 3L))
    },
    test("serializable transaction") {
      for count <-
          xa.withConnectionConfig(withSerializable).transact(carRepo.count)
      yield assertTrue(count == 3L)
    },
    test("failed transaction rolls back") {
      for
        result <- xa.transact {
          sql"delete from car where id = 1".update.run()
          throw IllegalStateException("intentional test failure")
        }.either
        count <- xa.connect(carRepo.count)
      yield assertTrue(result.isLeft, count == 3L)
    },
    test("select query") {
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
          .query[Car]
      for result <- xa.connect(query.run())
      yield assertTrue(
        query.frag.sqlString ==
          "select model, id, top_speed, vin, color, created from car where top_speed > ?",
        query.frag.params == Vector(minSpeed),
        result == allCars.tail
      )
    },
    test("select query with aliasing") {
      val minSpeed = 210
      val cAlias = car.alias("c")
      val query =
        sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
          .query[Car]
      for result <- xa.connect(query.run())
      yield assertTrue(
        query.frag.sqlString ==
          "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?",
        query.frag.params == Vector(minSpeed),
        result == allCars.tail
      )
    },
    test("select via option") {
      val vin = Option(124)
      for cars <- xa.connect(
          sql"select * from car where vin = $vin".query[Car].run()
        )
      yield assertTrue(cars == allCars.filter(_.vinNumber == vin))
    },
    test("tuple select") {
      for tuples <- xa.connect(
          sql"select model, color from car where id = 2"
            .query[(String, Color)]
            .run()
        )
      yield assertTrue(tuples == Vector(allCars(1).model -> allCars(1).color))
    },
    test(
      "large tuple support does not override hand-rolled Tuple[2-4] codecs"
    ) {
      for _ <- ZIO.unit
      yield
        val tuple2ACodec = summon[DbCodec[(String, Color)]]
        val tuple2BCodec = summon[DbCodec[(String, Int)]]
        val tuple5ACodec =
          summon[DbCodec[(String, Color, Int, Long, Option[Int])]]
        val tuple5BCodec = summon[DbCodec[(Int, Int, Int, Long, Option[Int])]]
        assertTrue(
          tuple2ACodec.getClass == tuple2BCodec.getClass,
          tuple5ACodec.getClass != tuple2ACodec.getClass,
          tuple5BCodec.getClass != tuple5ACodec.getClass
        )
    },
    test("large tuple select") {
      for tuple <- xa.connect(
          sql"select model, color, top_speed, id, vin from car where id = 2"
            .query[(String, Color, Int, Long, Option[Int])]
            .run()
            .head
        )
      yield
        val c = allCars(1)
        assertTrue(tuple == (c.model, c.color, c.topSpeed, c.id, c.vinNumber))
    },
    test("reads null int as None and not Some(0)") {
      for car <- xa.connect(carRepo.findById(3L))
      yield assertTrue(car.flatMap(_.vinNumber).isEmpty)
    },
    test("created timestamps should match") {
      for cars <- xa.connect(carRepo.findAll)
      yield assertTrue(cars.map(_.created) == allCars.map(_.created))
    },
    test(".query iterator") {
      for count <- xa.connect(
          Using
            .Manager(implicit use =>
              val it = sql"SELECT * FROM car".query[Car].iterator()
              it.map(_.id).size
            )
            .get
        )
      yield assertTrue(count == 3)
    }
  )

end PgZioTests
