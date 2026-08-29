package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.BatchUpdateResult
import munit.FunSuite

def compositeIdTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using munit.Location
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Point(@Id x: Int, @Id y: Int, descr: String) derives DbCodec

  case class PointId(x: Int, y: Int) derives DbCodec

  val pointRepo = Repo[Point, Point, PointId]
  val point = TableInfo[Point, Point, PointId]

  val pointRepoTupled = Repo[Point, Point, (Int, Int)]
  val pointTupled = TableInfo[Point, Point, (Int, Int)]

  val allPoints = Vector(
    Point(x = 1, y = 1, descr = "hello"),
    Point(x = 1, y = 2, descr = "world")
  )

  @SqlName("comp_id")
  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class CompIdRow(a: String, @Id b: Int, c: Int, @Id d: String)
      derives DbCodec

  case class CompId(b: Int, d: String) derives DbCodec

  val compIdRepo = Repo[CompIdRow, CompIdRow, CompId]
  val allCompIdRows = Vector(
    CompIdRow("alpha", 1, 10, "first"),
    CompIdRow("beta", 2, 20, "second"),
    CompIdRow("gamma", 3, 30, "third")
  )

  // ImmutableRepo methods

  test("composite id existsById"):
    xa().connect:
      assert(pointRepo.existsById(PointId(1, 1)))
      assert(pointRepo.existsById(PointId(1, 2)))
      assert(!pointRepo.existsById(PointId(2, 1)))

  test("composite id existsById (tupled)"):
    xa().connect:
      assert(pointRepoTupled.existsById((1, 1)))
      assert(pointRepoTupled.existsById((1, 2)))
      assert(!pointRepoTupled.existsById((2, 1)))

  test("composit id findAll (Spec)"):
    xa().connect:
      val spec = Spec[Point].where(sql"${point.x} = 1 AND ${point.y} = 1")
      assertEquals(pointRepo.findAll(spec), Vector(allPoints(0)))

  test("composit id findAll (Spec) (tupled)"):
    xa().connect:
      val spec =
        Spec[Point].where(sql"${pointTupled.x} = 1 AND ${pointTupled.y} = 1")
      assertEquals(pointRepoTupled.findAll(spec), Vector(allPoints(0)))

  test("composite id findById"):
    xa().connect:
      assert(pointRepo.findById(PointId(1, 1)).get == allPoints(0))
      assert(pointRepo.findById(PointId(1, 2)).get == allPoints(1))
      assert(pointRepo.findById(PointId(2, 2)).isEmpty)

  test("composite id findById (tupled)"):
    xa().connect:
      assert(pointRepoTupled.findById((1, 1)).get == allPoints(0))
      assert(pointRepoTupled.findById((1, 2)).get == allPoints(1))
      assert(pointRepoTupled.findById((2, 2)).isEmpty)

  test("non-leading composite id lookup"):
    xa().connect:
      assert(compIdRepo.existsById(CompId(1, "first")))
      assertEquals(
        compIdRepo.findById(CompId(2, "second")),
        Some(allCompIdRows(1))
      )
      assert(!compIdRepo.existsById(CompId(9, "missing")))
      assertEquals(compIdRepo.findById(CompId(9, "missing")), None)

  test("non-leading composite id findAllById"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      assertEquals(
        compIdRepo
          .findAllById(Vector(CompId(1, "first"), CompId(3, "third")))
          .toSet,
        Set(allCompIdRows(0), allCompIdRows(2))
      )
      assertEquals(compIdRepo.findAllById(Vector.empty), Vector.empty)

  test("composite id findAllById"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      assertEquals(
        pointRepo.findAllById(Vector(PointId(1, 1), PointId(1, 2))),
        allPoints
      )
      assertEquals(
        pointRepo.findAllById(Vector(PointId(1, 1), PointId(9, 9))),
        Vector(allPoints(0))
      )
      assertEquals(pointRepo.findAllById(Vector.empty), Vector.empty)

  test("composite id findAllById (tupled)"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      assertEquals(
        pointRepoTupled.findAllById(Vector((1, 1), (1, 2))),
        allPoints
      )
      assertEquals(
        pointRepoTupled.findAllById(Vector((1, 1), (9, 9))),
        Vector(allPoints(0))
      )
      assertEquals(pointRepoTupled.findAllById(Vector.empty), Vector.empty)

  // Repo methods

  test("composite id delete"):
    xa().connect:
      val p = pointRepo.findById(PointId(1, 1)).get
      pointRepo.delete(p)
      assert(pointRepo.count == 1L)
      assert(pointRepo.findById(PointId(1, 1)).isEmpty)

  test("composite id delete (tupled)"):
    xa().connect:
      val p = pointRepoTupled.findById((1, 1)).get
      pointRepoTupled.delete(p)
      assert(pointRepoTupled.count == 1L)
      assert(pointRepoTupled.findById((1, 1)).isEmpty)

  test("composite id deleteById"):
    xa().connect:
      pointRepo.deleteById(PointId(1, 1))
      assert(pointRepo.count == 1L)
      assert(pointRepo.findById(PointId(1, 1)).isEmpty)
      assert(pointRepo.findById(PointId(1, 2)).isDefined)

  test("composite id deleteById (tupled)"):
    xa().connect:
      pointRepoTupled.deleteById((1, 1))
      assert(pointRepoTupled.count == 1L)
      assert(pointRepoTupled.findById((1, 1)).isEmpty)
      assert(pointRepoTupled.findById((1, 2)).isDefined)

  test("non-leading composite id deleteById"):
    xa().connect:
      compIdRepo.deleteById(CompId(2, "second"))
      assertEquals(compIdRepo.count, 2L)
      assert(!compIdRepo.existsById(CompId(2, "second")))
      assertEquals(
        compIdRepo.findAll.toSet,
        Set(allCompIdRows(0), allCompIdRows(2))
      )

  test("composite id deleteAll"):
    xa().connect:
      val p1 = pointRepo.findById(PointId(1, 1)).get
      val p2 = pointRepo.findById(PointId(1, 2)).get
      val result = pointRepo.deleteAll(Vector(p1, p2))
      assert(result == BatchUpdateResult.Success(2))
      assert(pointRepo.count == 0L)

  test("composite id deleteAll (tupled)"):
    xa().connect:
      val p1 = pointRepoTupled.findById((1, 1)).get
      val p2 = pointRepoTupled.findById((1, 2)).get
      val result = pointRepoTupled.deleteAll(Vector(p1, p2))
      assert(result == BatchUpdateResult.Success(2))
      assert(pointRepoTupled.count == 0L)

  test("composite id deleteAllById"):
    xa().connect:
      val result = pointRepo.deleteAllById(Vector(PointId(1, 1), PointId(1, 2)))
      assert(result == BatchUpdateResult.Success(2))
      assert(pointRepo.count == 0L)
      assert(pointRepo.findAll.isEmpty)

  test("composite id deleteAllById (tupled)"):
    xa().connect:
      val result = pointRepoTupled.deleteAllById(Vector((1, 1), (1, 2)))
      assert(result == BatchUpdateResult.Success(2))
      assert(pointRepoTupled.count == 0L)
      assert(pointRepoTupled.findAll.isEmpty)

  test("composite id deleteAllById partial"):
    xa().connect:
      val result = pointRepo.deleteAllById(Vector(PointId(1, 1), PointId(9, 9)))
      if dbType == ClickhouseDbType then
        assertEquals(result, BatchUpdateResult.Success(2))
      else assertEquals(result, BatchUpdateResult.Success(1))
      assert(pointRepo.count == 1L)
      assert(pointRepo.findById(PointId(1, 1)).isEmpty)
      assert(pointRepo.findById(PointId(1, 2)).isDefined)

  test("composite id deleteAllById partial (tupled)"):
    xa().connect:
      val result = pointRepoTupled.deleteAllById(Vector((1, 1), (9, 9)))
      if dbType == ClickhouseDbType then
        assertEquals(result, BatchUpdateResult.Success(2))
      else assertEquals(result, BatchUpdateResult.Success(1))
      assert(pointRepoTupled.count == 1L)
      assert(pointRepoTupled.findById((1, 1)).isEmpty)
      assert(pointRepoTupled.findById((1, 2)).isDefined)

  test("composite id insert"):
    xa().connect:
      pointRepo.insert(Point(2, 1, "new point"))
      assert(pointRepo.count == 3L)
      val inserted = pointRepo.findById(PointId(2, 1)).get
      assert(inserted.x == 2)
      assert(inserted.y == 1)
      assert(inserted.descr == "new point")

  test("composite id insert (tupled)"):
    xa().connect:
      pointRepoTupled.insert(Point(2, 1, "new point"))
      assert(pointRepoTupled.count == 3L)
      val inserted = pointRepoTupled.findById((2, 1)).get
      assert(inserted.x == 2)
      assert(inserted.y == 1)
      assert(inserted.descr == "new point")

  test("composite id insertAll"):
    xa().connect:
      val newPoints = Vector(
        Point(2, 1, "point A"),
        Point(2, 2, "point B")
      )
      pointRepo.insertAll(newPoints)
      assert(pointRepo.count == 4L)
      // Use findById individually since findAllById doesn't support composite IDs on H2
      val p1 = pointRepo.findById(PointId(2, 1)).get
      val p2 = pointRepo.findById(PointId(2, 2)).get
      assert(p1.descr == "point A")
      assert(p2.descr == "point B")

  test("composite id insertAll (tupled)"):
    xa().connect:
      val newPoints = Vector(
        Point(2, 1, "point A"),
        Point(2, 2, "point B")
      )
      pointRepoTupled.insertAll(newPoints)
      assert(pointRepoTupled.count == 4L)
      val p1 = pointRepoTupled.findById((2, 1)).get
      val p2 = pointRepoTupled.findById((2, 2)).get
      assert(p1.descr == "point A")
      assert(p2.descr == "point B")

  test("composite id insertReturning"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val inserted = pointRepo.insertReturning(Point(2, 1, "returned"))
      assert(inserted.descr == "returned")
      assert(pointRepo.findById(PointId(2, 1)).get == inserted)

  test("composite id insertReturning (tupled)"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val inserted = pointRepoTupled.insertReturning(Point(2, 1, "returned"))
      assert(inserted.descr == "returned")
      assert(pointRepoTupled.findById((2, 1)).get == inserted)

  test("composite id insertAllReturning"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val newPoints = Vector(
        Point(2, 1, "first"),
        Point(2, 2, "second")
      )
      val returned = pointRepo.insertAllReturning(newPoints)
      assert(returned.size == 2)
      assert(returned.map(_.descr) == Vector("first", "second"))

  test("composite id insertAllReturning (tupled)"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val newPoints = Vector(
        Point(2, 1, "first"),
        Point(2, 2, "second")
      )
      val returned = pointRepoTupled.insertAllReturning(newPoints)
      assert(returned.size == 2)
      assert(returned.map(_.descr) == Vector("first", "second"))

  test("composite id update"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val p = pointRepo.findById(PointId(1, 1)).get
      val updated = p.copy(descr = "updated hello")
      pointRepo.update(updated)
      val fetched = pointRepo.findById(PointId(1, 1)).get
      assert(fetched.descr == "updated hello")
      assert(fetched.x == 1)
      assert(fetched.y == 1)
      assert(fetched == updated)

  test("composite id update (tupled)"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val p = pointRepoTupled.findById((1, 1)).get
      val updated = p.copy(descr = "updated hello")
      pointRepoTupled.update(updated)
      val fetched = pointRepoTupled.findById((1, 1)).get
      assert(fetched.descr == "updated hello")
      assert(fetched.x == 1)
      assert(fetched.y == 1)
      assert(fetched == updated)

  test("composite id updateAll"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val p1 = pointRepo.findById(PointId(1, 1)).get
      val p2 = pointRepo.findById(PointId(1, 2)).get
      val updated = Vector(
        p1.copy(descr = "new hello"),
        p2.copy(descr = "new world")
      )
      val result = pointRepo.updateAll(updated)
      assert(result == BatchUpdateResult.Success(2))
      assert(pointRepo.findById(PointId(1, 1)).get.descr == "new hello")
      assert(pointRepo.findById(PointId(1, 2)).get.descr == "new world")

  test("composite id updateAll (tupled)"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val p1 = pointRepoTupled.findById((1, 1)).get
      val p2 = pointRepoTupled.findById((1, 2)).get
      val updated = Vector(
        p1.copy(descr = "new hello"),
        p2.copy(descr = "new world")
      )
      val result = pointRepoTupled.updateAll(updated)
      assert(result == BatchUpdateResult.Success(2))
      assert(pointRepoTupled.findById((1, 1)).get.descr == "new hello")
      assert(pointRepoTupled.findById((1, 2)).get.descr == "new world")

  // Sql interpolator tests

  test("composite id table info aliasing"):
    xa().connect:
      val pAlias = point.alias("p")
      // Verify the SQL is generated correctly with alias
      val query =
        sql"select ${pAlias.all} from $pAlias where ${pAlias.x} = 1 and ${pAlias.y} = 1"
      assert(query.sqlString.contains("p.x") || query.sqlString.contains("p.y"))
      val points = query.query[Point].run()
      assert(points == Vector(allPoints(0)))

  test("composite id table info aliasing (tupled)"):
    xa().connect:
      val pAlias = pointTupled.alias("p")
      // Verify the SQL is generated correctly with alias
      val query =
        sql"select ${pAlias.all} from $pAlias where ${pAlias.x} = 1 and ${pAlias.y} = 1"
      assert(query.sqlString.contains("p.x") || query.sqlString.contains("p.y"))
      val points = query.query[Point].run()
      assert(points == Vector(allPoints(0)))

  test("composite id TableInfo idIndices"):
    xa().connect:
      assert(
        point.idColumns.columnNames
          .map(_.scalaName)
          .sameElements(Vector("x", "y"))
      )

  test("composite id TableInfo idIndices (tupled)"):
    xa().connect:
      assert(
        pointTupled.idColumns.columnNames
          .map(_.scalaName)
          .sameElements(Vector("x", "y"))
      )

end compositeIdTests
