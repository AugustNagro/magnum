package shared

import com.augustnagro.magnum.common.*
import munit.{FunSuite, Location}

def multilineFragTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using Location
): Unit =
  import suite.*

  def testSelectPersonFrag(frag: Frag): Unit =
    xa().connect:
      val res = frag.query[Int].run()
      assert(res.size == 1)
      assert(frag.sqlString == "SELECT count(*)\nFROM person\nWHERE id = ?")
      assert(frag.params.size == 1)

  def personId = util.Random.nextInt(8) + 1

  test("multiline Frag"):
    testSelectPersonFrag(
      frag = sql"""SELECT count(*)
FROM person
WHERE id = $personId"""
    )

  test("multiline Frag with stripMargin"):
    testSelectPersonFrag(
      frag = sql"""SELECT count(*)
                  |FROM person
                  |WHERE id = $personId""".stripMargin
    )

  test("multiline Frag with stripMargin('*')"):
    testSelectPersonFrag(
      frag = sql"""SELECT count(*)
                  *FROM person
                  *WHERE id = $personId""".stripMargin('*')
    )

end multilineFragTests
