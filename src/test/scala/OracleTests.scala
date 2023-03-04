import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.FunSuite
import org.testcontainers.utility.DockerImageName
import oracle.jdbc.datasource.impl.OracleDataSource

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.OffsetDateTime
import javax.sql.DataSource
import scala.util.Using

class OracleTests extends FunSuite, TestContainersFixtures:

  /*
  Immutable Repo Tests
   */

  case class Car(model: String, @Id id: Long, topSpeed: Int) derives DbReader

  val carSchema = DbSchema[Car, Car, Long](SqlNameMapper.CamelToSnakeCase)

  val carRepo = ImmutableRepo(carSchema)

  val allCars = Vector(
    Car("McLaren Senna", 1L, 208),
    Car("Ferrari F8 Tributo", 2L, 212),
    Car("Aston Martin Superleggera", 3L, 211)
  )

  test("count"):
    connect(ds()):
      assertEquals(carRepo.count, 3L)

  test("existsById"):
    connect(ds()):
      assert(carRepo.existsById(3L))
      assert(!carRepo.existsById(4L))

  test("findAll"):
    val cars = connect(ds()):
      carRepo.findAll
    assertEquals(cars, allCars)

  test("findAll spec"):
    connect(ds()):
      val spec = Spec(carSchema)
        .where(sql"${carSchema.topSpeed} > 211")
      assertEquals(carRepo.findAll(spec), Vector(allCars(1)))

  test("findById"):
    connect(ds()):
      assertEquals(carRepo.findById(3L).get, allCars.last)
      assertEquals(carRepo.findById(4L), None)

  test("findAllByIds"):
    connect(ds()):
      assertEquals(
        carRepo.findAllById(Vector(1L, 3L)).map(_.id),
        Vector(1L, 3L)
      )

  test("repeatable read transaction"):
    transact(ds(), withRepeatableRead):
      assertEquals(carRepo.count, 3L)

  private def withRepeatableRead(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)

  test("select query"):
    connect(ds()):
      val car = carSchema
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"

      assertNoDiff(
        query.query,
        "select model, id, top_speed from car where top_speed > ?"
      )
      assertEquals(query.params, Vector(minSpeed))
      assertEquals(
        query.run[Car],
        allCars.tail
      )

  test("select query with aliasing"):
    connect(ds()):
      val car = carSchema.alias("c")
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"

      assertNoDiff(
        query.query,
        "select c.model, c.id, c.top_speed from car c where c.top_speed > ?"
      )
      assertEquals(query.run[Car], allCars.tail)

  val oracleContainer = ForAllContainerFixture(
    OracleContainer
      .Def(dockerImageName =
        DockerImageName.parse(
          "gvenzl/oracle-xe:21.3.0"
        )
      )
      .createContainer()
  )

  override def munitFixtures: Seq[Fixture[_]] =
    super.munitFixtures :+ oracleContainer

  def ds(): DataSource =
    val oracle = oracleContainer()
    val ds = OracleDataSource()
    ds.setURL(oracle.jdbcUrl)
    ds.setUser(oracle.username)
    ds.setPassword(oracle.password)
    Using.resource(ds.getConnection()): con =>
      val stmt = con.createStatement()
      try stmt.execute("drop table car")
      catch case _ => ()
      stmt.execute(
        """create table car (
          |  model varchar2(50) not null,
          |  id number generated always as identity,
          |  top_speed number
          |)""".stripMargin
      )
      stmt.execute(
        """insert into car (model, top_speed)
          |values ('McLaren Senna', 208)""".stripMargin
      )
      stmt.execute(
        """insert into car (model, top_speed)
          |values ('Ferrari F8 Tributo', 212)""".stripMargin
      )
      stmt.execute(
        """insert into car (model, top_speed)
          |values ('Aston Martin Superleggera', 211)""".stripMargin
      )
    ds
