import munit.FunSuite
import org.postgresql.ds.PGSimpleDataSource
import com.augustnagro.magnum.{
  CamelToSnakeCase,
  DbReader,
  DbSchema,
  ImmutableRepo,
  connect,
  sql,
  transact
}

import java.nio.file.{Files, Path}
import javax.sql.DataSource
import scala.util.Using

class ImmutableRepoTests extends FunSuite {

  case class Car(id: Long, model: String, topSpeed: Int) derives DbReader

  case class CarCreator(model: String, topSpeed: Int)

  val userSchema = DbSchema[CarCreator, Car, Long](CamelToSnakeCase)

  val carRepo = ImmutableRepo(userSchema)

  val allCars = Vector(
    Car(1L, "McLaren Senna", 208),
    Car(2L, "Ferrari F8 Tributo", 212),
    Car(3L, "Aston Martin Superleggera", 211)
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

  test("findAllByIds") {
    connect(ds()) {
      assertEquals(
        Vector(1L, 3L),
        carRepo.findAllById(Vector(1L, 3L)).map(_.id)
      )
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

}
