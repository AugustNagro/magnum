package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.builders.{DbCon, Frag, Transactor}
import com.augustnagro.magnum.dbtype.{DbType, OracleDbType}
import munit.FunSuite

import java.util.UUID

def embeddedFragTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using munit.Location
): Unit =
  import suite.*

  test("embed Frag into Frag"):
    def findPersonCnt(filter: Frag)(using DbCon): Int =
      val x = sql"id != ${util.Random.nextInt(20) + 20}"
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

  test("embedded frag param exprs should be evaluated only once"):
    object Holder:
      var uuid: UUID = _
      def set(uuid: UUID): UUID =
        this.uuid = uuid
        uuid
    val frag =
      sql"select * from person where ${sql"social_id = ${Holder.set(UUID.randomUUID)}"}"
    assert(frag.params.size == 1)
    assert(frag.params.head == Holder.uuid)
end embeddedFragTests
