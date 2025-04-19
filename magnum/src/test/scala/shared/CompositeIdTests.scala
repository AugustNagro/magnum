package shared

import com.augustnagro.magnum.*
import munit.FunSuite

// todo try to use as ID type
opaque type PointId = (Int, Int)
object PointId:
  def apply(x: Int, y: Int): PointId = (x, y)
  extension (pid: PointId)
    def x: Int = pid._1
    def y: Int = pid._2

def compositeIdTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using munit.Location
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Point(@Id x: Int, @Id y: Int, descr: String) derives DbCodec

  val pointRepo = Repo[Point, Point, (Int, Int)]

  test("find by composite id".only):
    xa().connect:
      assert(pointRepo.findById((1, 1)).isDefined)
      assert(pointRepo.findById((1, 0)).isEmpty)
