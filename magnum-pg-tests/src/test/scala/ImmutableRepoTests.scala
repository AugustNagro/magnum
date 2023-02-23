import munit.FunSuite
import org.postgresql.ds.PGSimpleDataSource
import com.augustnagro.magnum.{
  CamelToSnakeCase,
  DbReader,
  DbSchema,
  ImmutableRepo,
  connect,
  sql,
  transact,
  Id
}

import java.nio.file.{Files, Path}
import java.sql.Connection
import javax.sql.DataSource
import scala.util.Using

class ImmutableRepoTests extends FunSuite {

  case class Car(model: String, @Id id: Long, topSpeed: Int) derives DbReader

  val carSchema = DbSchema[Car, Car, Long](CamelToSnakeCase)

  val carRepo = ImmutableRepo(carSchema)

  val allCars = Vector(
    Car("McLaren Senna", 1L, 208),
    Car("Ferrari F8 Tributo", 2L, 212),
    Car("Aston Martin Superleggera", 3L, 211)
  )

  test("count") {
    val count = connect(ds())(carRepo.count)
    assertEquals(count, 3L)
  }

  test("existsById") {
    connect(ds()) {
      assert(carRepo.existsById(3L))
      assert(!carRepo.existsById(4L))
    }
  }

  test("findAll") {
    connect(ds()) {
      assertEquals(carRepo.findAll, allCars)
    }
  }

  // todo findAll Spec

  test("findById") {
    connect(ds()) {
      assertEquals(carRepo.findById(3L).get, allCars.last)
      assertEquals(carRepo.findById(4L), None)
    }
  }

  test("findAllByIds") {
    connect(ds()) {
      assertEquals(
        carRepo.findAllById(Vector(1L, 3L)).map(_.id),
        Vector(1L, 3L)
      )
    }
  }

  test("repeatable read transaction") {
    transact(ds(), withRepeatableRead) {
      assertEquals(carRepo.count, 3L)
    }
  }

  test("select query") {
    connect(ds()) {
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
    }
  }

  test("select query with aliasing") {
    connect(ds()) {
      val car = carSchema.alias("c")
      val minSpeed = 210
      val query =
        sql"select ${car.all} from $car where ${car.topSpeed} > $minSpeed"

      assertNoDiff(
        query.query,
        "select c.model, c.id, c.top_speed from car c where c.top_speed > ?"
      )

      assertEquals(query.run[Car], allCars.tail)
    }
  }

  private def ds(): DataSource =
    val ds = PGSimpleDataSource()
    ds.setServerNames(Array(PgConfig.Db.host))
    ds.setDatabaseName(PgConfig.Db.name)
    ds.setUser(PgConfig.Db.user)
    ds.setPassword(PgConfig.Db.password)
    val testSql =
      Files.readString(Path.of(getClass.getResource("/car.sql").toURI))
    Using.resource(ds.getConnection)(con =>
      con.prepareStatement(testSql).execute()
    )
    ds

  private def withRepeatableRead(con: Connection): Unit =
    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)

}
