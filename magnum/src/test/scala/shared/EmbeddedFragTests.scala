package shared

import com.augustnagro.magnum.*
import munit.FunSuite

def embeddedFragTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using munit.Location
): Unit =
  import suite.*

  test("embed Frag into Frag"):
    def findPersonCnt(filter: Frag)(using DbCon): Int =
      val x = sql"first_name IS NOT NULL"
      sql"SELECT count(*) FROM person WHERE $filter AND $x"
        .query[Int]
        .run()
        .head
    val isAdminFrag =
      if dbType == OracleDbType then sql"is_admin = 'Y'"
      else sql"is_admin = true"
    connect(xa()):
      val johnCnt =
        findPersonCnt(sql"$isAdminFrag AND first_name = 'John'")
      assert(johnCnt == 2)
