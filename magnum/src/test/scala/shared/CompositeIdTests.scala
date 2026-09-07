package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.BatchUpdateResult
import munit.FunSuite

def compositeIdTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using munit.Location
): Unit =
  import suite.*

  @SqlName("comp_id")
  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class CompIdRow(a: String, @Id b: Int, c: Int, @Id d: String)
      derives DbCodec

  case class CompId(b: Int, d: String) derives DbCodec

  val compIdRepo = Repo[CompIdRow, CompIdRow, CompId]
  val compId = TableInfo[CompIdRow, CompIdRow, CompId]
  val compIdRepoTupled = Repo[CompIdRow, CompIdRow, (Int, String)]
  val compIdTupled = TableInfo[CompIdRow, CompIdRow, (Int, String)]

  val allCompIdRows = Vector(
    CompIdRow("alpha", 1, 10, "first"),
    CompIdRow("beta", 2, 20, "second"),
    CompIdRow("gamma", 3, 30, "third")
  )

  // ImmutableRepo methods

  test("composite id existsById"):
    xa().connect:
      assert(compIdRepo.existsById(CompId(1, "first")))
      assert(compIdRepo.existsById(CompId(2, "second")))
      assert(!compIdRepo.existsById(CompId(9, "missing")))

  test("composite id existsById (tupled)"):
    xa().connect:
      assert(compIdRepoTupled.existsById((1, "first")))
      assert(compIdRepoTupled.existsById((2, "second")))
      assert(!compIdRepoTupled.existsById((9, "missing")))

  test("composite id findAll (Spec)"):
    xa().connect:
      val spec =
        Spec[CompIdRow].where(sql"${compId.b} = 1 AND ${compId.d} = 'first'")
      assertEquals(compIdRepo.findAll(spec), Vector(allCompIdRows(0)))

  test("composite id findAll (Spec) (tupled)"):
    xa().connect:
      val spec =
        Spec[CompIdRow].where(
          sql"${compIdTupled.b} = 1 AND ${compIdTupled.d} = 'first'"
        )
      assertEquals(compIdRepoTupled.findAll(spec), Vector(allCompIdRows(0)))

  test("composite id findById"):
    xa().connect:
      assert(compIdRepo.findById(CompId(1, "first")).get == allCompIdRows(0))
      assert(compIdRepo.findById(CompId(2, "second")).get == allCompIdRows(1))
      assert(compIdRepo.findById(CompId(9, "missing")).isEmpty)

  test("composite id findById (tupled)"):
    xa().connect:
      assert(compIdRepoTupled.findById((1, "first")).get == allCompIdRows(0))
      assert(compIdRepoTupled.findById((2, "second")).get == allCompIdRows(1))
      assert(compIdRepoTupled.findById((9, "missing")).isEmpty)

  test("composite id findAllById"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      assertEquals(
        compIdRepo.findAllById(
          Vector(CompId(1, "first"), CompId(2, "second"))
        ),
        Vector(allCompIdRows(0), allCompIdRows(1))
      )
      assertEquals(
        compIdRepo.findAllById(
          Vector(CompId(1, "first"), CompId(9, "missing"))
        ),
        Vector(allCompIdRows(0))
      )
      assertEquals(compIdRepo.findAllById(Vector.empty), Vector.empty)

  test("composite id findAllById (tupled)"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != OracleDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      assertEquals(
        compIdRepoTupled.findAllById(
          Vector((1, "first"), (2, "second"))
        ),
        Vector(allCompIdRows(0), allCompIdRows(1))
      )
      assertEquals(
        compIdRepoTupled.findAllById(
          Vector((1, "first"), (9, "missing"))
        ),
        Vector(allCompIdRows(0))
      )
      assertEquals(compIdRepoTupled.findAllById(Vector.empty), Vector.empty)

  // Repo methods

  test("composite id delete"):
    xa().connect:
      val row = compIdRepo.findById(CompId(1, "first")).get
      compIdRepo.delete(row)
      assert(compIdRepo.count == 2L)
      assert(compIdRepo.findById(CompId(1, "first")).isEmpty)

  test("composite id delete (tupled)"):
    xa().connect:
      val row = compIdRepoTupled.findById((1, "first")).get
      compIdRepoTupled.delete(row)
      assert(compIdRepoTupled.count == 2L)
      assert(compIdRepoTupled.findById((1, "first")).isEmpty)

  test("composite id deleteById"):
    xa().connect:
      compIdRepo.deleteById(CompId(1, "first"))
      assert(compIdRepo.count == 2L)
      assert(compIdRepo.findById(CompId(1, "first")).isEmpty)
      assert(compIdRepo.findById(CompId(2, "second")).isDefined)

  test("composite id deleteById (tupled)"):
    xa().connect:
      compIdRepoTupled.deleteById((1, "first"))
      assert(compIdRepoTupled.count == 2L)
      assert(compIdRepoTupled.findById((1, "first")).isEmpty)
      assert(compIdRepoTupled.findById((2, "second")).isDefined)

  test("composite id deleteAll"):
    xa().connect:
      val row1 = compIdRepo.findById(CompId(1, "first")).get
      val row2 = compIdRepo.findById(CompId(2, "second")).get
      val result = compIdRepo.deleteAll(Vector(row1, row2))
      assert(result == BatchUpdateResult.Success(2))
      assert(compIdRepo.count == 1L)

  test("composite id deleteAll (tupled)"):
    xa().connect:
      val row1 = compIdRepoTupled.findById((1, "first")).get
      val row2 = compIdRepoTupled.findById((2, "second")).get
      val result = compIdRepoTupled.deleteAll(Vector(row1, row2))
      assert(result == BatchUpdateResult.Success(2))
      assert(compIdRepoTupled.count == 1L)

  test("composite id deleteAllById"):
    xa().connect:
      val result = compIdRepo.deleteAllById(
        Vector(CompId(1, "first"), CompId(2, "second"))
      )
      assert(result == BatchUpdateResult.Success(2))
      assert(compIdRepo.count == 1L)
      assertEquals(compIdRepo.findAll, Vector(allCompIdRows(2)))

  test("composite id deleteAllById (tupled)"):
    xa().connect:
      val result = compIdRepoTupled.deleteAllById(
        Vector((1, "first"), (2, "second"))
      )
      assert(result == BatchUpdateResult.Success(2))
      assert(compIdRepoTupled.count == 1L)
      assertEquals(compIdRepoTupled.findAll, Vector(allCompIdRows(2)))

  test("composite id deleteAllById partial"):
    xa().connect:
      val result = compIdRepo.deleteAllById(
        Vector(CompId(1, "first"), CompId(9, "missing"))
      )
      if dbType == ClickhouseDbType then
        assertEquals(result, BatchUpdateResult.Success(2))
      else assertEquals(result, BatchUpdateResult.Success(1))
      assert(compIdRepo.count == 2L)
      assert(compIdRepo.findById(CompId(1, "first")).isEmpty)
      assert(compIdRepo.findById(CompId(2, "second")).isDefined)

  test("composite id deleteAllById partial (tupled)"):
    xa().connect:
      val result = compIdRepoTupled.deleteAllById(
        Vector((1, "first"), (9, "missing"))
      )
      if dbType == ClickhouseDbType then
        assertEquals(result, BatchUpdateResult.Success(2))
      else assertEquals(result, BatchUpdateResult.Success(1))
      assert(compIdRepoTupled.count == 2L)
      assert(compIdRepoTupled.findById((1, "first")).isEmpty)
      assert(compIdRepoTupled.findById((2, "second")).isDefined)

  test("composite id insert"):
    xa().connect:
      compIdRepo.insert(CompIdRow("delta", 4, 40, "fourth"))
      assert(compIdRepo.count == 4L)
      val inserted = compIdRepo.findById(CompId(4, "fourth")).get
      assert(inserted.a == "delta")
      assert(inserted.b == 4)
      assert(inserted.c == 40)
      assert(inserted.d == "fourth")

  test("composite id insert (tupled)"):
    xa().connect:
      compIdRepoTupled.insert(CompIdRow("delta", 4, 40, "fourth"))
      assert(compIdRepoTupled.count == 4L)
      val inserted = compIdRepoTupled.findById((4, "fourth")).get
      assert(inserted.a == "delta")
      assert(inserted.b == 4)
      assert(inserted.c == 40)
      assert(inserted.d == "fourth")

  test("composite id insertAll"):
    xa().connect:
      val newRows = Vector(
        CompIdRow("delta", 4, 40, "fourth"),
        CompIdRow("epsilon", 5, 50, "fifth")
      )
      compIdRepo.insertAll(newRows)
      assert(compIdRepo.count == 5L)
      val row1 = compIdRepo.findById(CompId(4, "fourth")).get
      val row2 = compIdRepo.findById(CompId(5, "fifth")).get
      assert(row1.a == "delta")
      assert(row2.a == "epsilon")

  test("composite id insertAll (tupled)"):
    xa().connect:
      val newRows = Vector(
        CompIdRow("delta", 4, 40, "fourth"),
        CompIdRow("epsilon", 5, 50, "fifth")
      )
      compIdRepoTupled.insertAll(newRows)
      assert(compIdRepoTupled.count == 5L)
      val row1 = compIdRepoTupled.findById((4, "fourth")).get
      val row2 = compIdRepoTupled.findById((5, "fifth")).get
      assert(row1.a == "delta")
      assert(row2.a == "epsilon")

  test("composite id insertReturning"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val inserted =
        compIdRepo.insertReturning(CompIdRow("delta", 4, 40, "fourth"))
      assert(inserted.a == "delta")
      assert(compIdRepo.findById(CompId(4, "fourth")).get == inserted)

  test("composite id insertReturning (tupled)"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val inserted = compIdRepoTupled.insertReturning(
        CompIdRow("delta", 4, 40, "fourth")
      )
      assert(inserted.a == "delta")
      assert(compIdRepoTupled.findById((4, "fourth")).get == inserted)

  test("composite id insertAllReturning"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val newRows = Vector(
        CompIdRow("delta", 4, 40, "fourth"),
        CompIdRow("epsilon", 5, 50, "fifth")
      )
      val returned = compIdRepo.insertAllReturning(newRows)
      assert(returned.size == 2)
      assert(returned.map(_.a) == Vector("delta", "epsilon"))

  test("composite id insertAllReturning (tupled)"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val newRows = Vector(
        CompIdRow("delta", 4, 40, "fourth"),
        CompIdRow("epsilon", 5, 50, "fifth")
      )
      val returned = compIdRepoTupled.insertAllReturning(newRows)
      assert(returned.size == 2)
      assert(returned.map(_.a) == Vector("delta", "epsilon"))

  test("composite id update"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val row = compIdRepo.findById(CompId(1, "first")).get
      val updated = row.copy(a = "updated alpha")
      compIdRepo.update(updated)
      val fetched = compIdRepo.findById(CompId(1, "first")).get
      assert(fetched.a == "updated alpha")
      assert(fetched.b == 1)
      assert(fetched.d == "first")
      assert(fetched == updated)

  test("composite id update (tupled)"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val row = compIdRepoTupled.findById((1, "first")).get
      val updated = row.copy(a = "updated alpha")
      compIdRepoTupled.update(updated)
      val fetched = compIdRepoTupled.findById((1, "first")).get
      assert(fetched.a == "updated alpha")
      assert(fetched.b == 1)
      assert(fetched.d == "first")
      assert(fetched == updated)

  test("composite id updateAll"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val row1 = compIdRepo.findById(CompId(1, "first")).get
      val row2 = compIdRepo.findById(CompId(2, "second")).get
      val updated = Vector(
        row1.copy(a = "updated alpha"),
        row2.copy(a = "updated beta")
      )
      val result = compIdRepo.updateAll(updated)
      assert(result == BatchUpdateResult.Success(2))
      assert(compIdRepo.findById(CompId(1, "first")).get.a == "updated alpha")
      assert(compIdRepo.findById(CompId(2, "second")).get.a == "updated beta")

  test("composite id updateAll (tupled)"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val row1 = compIdRepoTupled.findById((1, "first")).get
      val row2 = compIdRepoTupled.findById((2, "second")).get
      val updated = Vector(
        row1.copy(a = "updated alpha"),
        row2.copy(a = "updated beta")
      )
      val result = compIdRepoTupled.updateAll(updated)
      assert(result == BatchUpdateResult.Success(2))
      assert(
        compIdRepoTupled.findById((1, "first")).get.a == "updated alpha"
      )
      assert(
        compIdRepoTupled.findById((2, "second")).get.a == "updated beta"
      )

  // Sql interpolator tests

  test("composite id table info aliasing"):
    xa().connect:
      val rowAlias = compId.alias("r")
      // Verify the SQL is generated correctly with alias
      val query =
        sql"select ${rowAlias.all} from $rowAlias where ${rowAlias.b} = 1 and ${rowAlias.d} = 'first'"
      assert(query.sqlString.contains("r.b"))
      assert(query.sqlString.contains("r.d"))
      val rows = query.query[CompIdRow].run()
      assert(rows == Vector(allCompIdRows(0)))

  test("composite id table info aliasing (tupled)"):
    xa().connect:
      val rowAlias = compIdTupled.alias("r")
      // Verify the SQL is generated correctly with alias
      val query =
        sql"select ${rowAlias.all} from $rowAlias where ${rowAlias.b} = 1 and ${rowAlias.d} = 'first'"
      assert(query.sqlString.contains("r.b"))
      assert(query.sqlString.contains("r.d"))
      val rows = query.query[CompIdRow].run()
      assert(rows == Vector(allCompIdRows(0)))

  test("composite id TableInfo idIndices"):
    xa().connect:
      assert(
        compId.idColumns.columnNames
          .map(_.scalaName)
          .sameElements(Vector("b", "d"))
      )

  test("composite id TableInfo idIndices (tupled)"):
    xa().connect:
      assert(
        compIdTupled.idColumns.columnNames
          .map(_.scalaName)
          .sameElements(Vector("b", "d"))
      )

end compositeIdTests
