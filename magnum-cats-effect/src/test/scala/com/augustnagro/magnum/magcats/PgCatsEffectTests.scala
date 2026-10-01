package com.augustnagro.magnum.magcats

import cats.effect.IO
import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, CatsEffectSuite}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.time.OffsetDateTime
import scala.util.Using
import scala.util.Using.Manager

class PgCatsEffectTests extends CatsEffectSuite, TestContainersFixtures:

  private val pgContainer = new ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:18.6"))
      .createContainer()
  ):
    override def afterContainerStart(container: PostgreSQLContainer): Unit =
      initializeSchema(dataSource(container))

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ pgContainer

  private def dataSource(container: PostgreSQLContainer): PGSimpleDataSource =
    val ds = PGSimpleDataSource()
    ds.setUrl(container.jdbcUrl)
    ds.setUser(container.username)
    ds.setPassword(container.password)
    ds

  private lazy val xa: TransactorCats[IO] =
    TransactorCats[IO](dataSource(pgContainer()))

  private def initializeSchema(ds: PGSimpleDataSource): Unit =
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
      val connection = use(ds.getConnection())
      val statement = use(connection.createStatement())
      tableDDLs.foreach(ddl => statement.execute(ddl))
    ).get

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

  private val carRepo = ImmutableRepo[Car, Long]
  private val car = TableInfo[Car, Car, Long]

  private val allCars = Vector(
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

  private def withSerializable(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

  test("count") {
    xa.connect(carRepo.count).map(count => assertEquals(count, 3L))
  }

  test("existsById") {
    xa.connect((carRepo.existsById(3L), carRepo.existsById(4L)))
      .map((exists3, exists4) =>
        assert(exists3)
        assert(!exists4)
      )
  }

  test("findAll") {
    xa.connect(carRepo.findAll).map(cars => assertEquals(cars, allCars))
  }

  test("findById") {
    xa.connect((carRepo.findById(3L), carRepo.findById(4L)))
      .map((car3, car4) =>
        assertEquals(car3, Some(allCars.last))
        assertEquals(car4, None)
      )
  }

  test("findAllByIds") {
    xa.connect(carRepo.findAllById(Vector(1L, 3L)).map(_.id))
      .map(ids => assertEquals(ids, Vector(1L, 3L)))
  }

  test("serializable transaction") {
    xa.withConnectionConfig(withSerializable)
      .transact(carRepo.count)
      .map(count => assertEquals(count, 3L))
  }

  test("failed transaction rolls back") {
    for
      result <- xa.transact {
        sql"delete from car where id = 1".update.run()
        throw IllegalStateException("intentional test failure")
      }.attempt
      count <- xa.connect(carRepo.count)
    yield
      assert(result.isLeft)
      assertEquals(count, 3L)
  }

  test("select query") {
    val minSpeed = 210
    val query =
      sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"
        .query[Car]
    xa.connect(query.run())
      .map(result =>
        assertEquals(
          query.frag.sqlString,
          "select model, id, top_speed, vin, color, created from car where top_speed > ?"
        )
        assertEquals(query.frag.params, Vector(minSpeed))
        assertEquals(result, allCars.tail)
      )
  }

  test("select query with aliasing") {
    val minSpeed = 210
    val cAlias = car.alias("c")
    val query =
      sql"select ${cAlias.all} from $cAlias where ${cAlias.topSpeed} > $minSpeed"
        .query[Car]
    xa.connect(query.run())
      .map(result =>
        assertEquals(
          query.frag.sqlString,
          "select c.model, c.id, c.top_speed, c.vin, c.color, c.created from car c where c.top_speed > ?"
        )
        assertEquals(query.frag.params, Vector(minSpeed))
        assertEquals(result, allCars.tail)
      )
  }

  test("select via option") {
    val vin = Option(124)
    xa.connect(sql"select * from car where vin = $vin".query[Car].run())
      .map(cars => assertEquals(cars, allCars.filter(_.vinNumber == vin)))
  }

  test("tuple select") {
    xa.connect(
      sql"select model, color from car where id = 2"
        .query[(String, Color)]
        .run()
    ).map(tuples =>
      assertEquals(tuples, Vector(allCars(1).model -> allCars(1).color))
    )
  }

  test("large tuple support does not override hand-rolled Tuple[2-4] codecs") {
    IO {
      val tuple2ACodec = summon[DbCodec[(String, Color)]]
      val tuple2BCodec = summon[DbCodec[(String, Int)]]
      val tuple5ACodec =
        summon[DbCodec[(String, Color, Int, Long, Option[Int])]]
      val tuple5BCodec = summon[DbCodec[(Int, Int, Int, Long, Option[Int])]]
      assert(tuple2ACodec.getClass == tuple2BCodec.getClass)
      assert(tuple5ACodec.getClass != tuple2ACodec.getClass)
      assert(tuple5BCodec.getClass != tuple5ACodec.getClass)
    }
  }

  test("large tuple select") {
    xa.connect(
      sql"select model, color, top_speed, id, vin from car where id = 2"
        .query[(String, Color, Int, Long, Option[Int])]
        .run()
        .head
    ).map(tuple =>
      val c = allCars(1)
      assertEquals(tuple, (c.model, c.color, c.topSpeed, c.id, c.vinNumber))
    )
  }

  test("reads null int as None and not Some(0)") {
    xa.connect(carRepo.findById(3L))
      .map(car => assert(car.flatMap(_.vinNumber).isEmpty))
  }

  test("created timestamps should match") {
    xa.connect(carRepo.findAll)
      .map(cars => assertEquals(cars.map(_.created), allCars.map(_.created)))
  }

  test(".query iterator") {
    xa.connect(
      Using
        .Manager(implicit use =>
          val it = sql"SELECT * FROM car".query[Car].iterator()
          it.map(_.id).size
        )
        .get
    ).map(count => assertEquals(count, 3))
  }

end PgCatsEffectTests
