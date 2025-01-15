package shared

import com.augustnagro.magnum.common.*
import com.augustnagro.magnum.SqlException
import munit.{FunSuite, Location}

import scala.util.Using

def entityCreatorTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(
    using Location
): Unit =
  import suite.*
  if dbType == ClickhouseDbType then return

  case class MyUserCreator(firstName: String) derives DbCodec

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class MyUser(firstName: String, id: Long) derives DbCodec

  val userRepo = Repo[MyUserCreator, MyUser, Long]
  val user = TableInfo[MyUserCreator, MyUser, Long]

  test("insert EntityCreator"):
    xa().connect:
      userRepo.insert(MyUserCreator("Ash"))
      userRepo.insert(MyUserCreator("Steve"))
      assert(userRepo.count == 5L)
      assert(userRepo.findAll.map(_.firstName).contains("Steve"))

  test("insertReturning EntityCreator"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val user = userRepo.insertReturning(MyUserCreator("Ash"))
      assert(user.firstName == "Ash")

  test("insertAllReturning EntityCreator"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val newUsers = Vector(
        MyUserCreator("Ash"),
        MyUserCreator("Steve"),
        MyUserCreator("Josh")
      )
      val users = userRepo.insertAllReturning(newUsers)
      assert(userRepo.count == 6L)
      assert(users.size == 3)
      assert(users.last.firstName == newUsers.last.firstName)

  test("insert invalid EntityCreator"):
    intercept[SqlException]:
      xa().connect:
        val invalidUser = MyUserCreator(null)
        userRepo.insert(invalidUser)

  test("insertAll EntityCreator"):
    xa().connect:
      val newUsers = Vector(
        MyUserCreator("Ash"),
        MyUserCreator("Steve"),
        MyUserCreator("Josh")
      )
      userRepo.insertAll(newUsers)
      assert(userRepo.count == 6L)
      assert(
        userRepo.findAll.map(_.firstName).contains(newUsers.last.firstName)
      )

  test("custom insert EntityCreator"):
    xa().connect:
      val u = MyUserCreator("Ash")
      val update =
        sql"insert into $user ${user.insertColumns} values ($u)".update
      assertNoDiff(
        update.frag.sqlString,
        "insert into my_user (first_name) values (?)"
      )
      val rowsInserted = update.run()
      assert(rowsInserted == 1)
      assert(userRepo.count == 4L)
      assert(userRepo.findAll.exists(_.firstName == "Ash"))

  test("custom update EntityCreator"):
    xa().connect:
      val u = userRepo.findAll.head
      val newName = "Ash"
      val update =
        sql"update $user set ${user.firstName} = $newName where ${user.id} = ${u.id}".update
      assertNoDiff(
        update.frag.sqlString,
        "update my_user set first_name = ? where id = ?"
      )
      val rowsUpdated = update.run()
      assert(rowsUpdated == 1)
      assert(userRepo.findAll.exists(_.firstName == "Ash"))

  test(".returning iterator"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      Using.Manager(implicit use =>
        val it =
          if dbType == H2DbType then
            sql"INSERT INTO $user ${user.insertColumns} VALUES ('Bob')"
              .returningKeys[Long](user.id)
              .iterator()
          else
            sql"INSERT INTO $user ${user.insertColumns} VALUES ('Bob') RETURNING ${user.id}"
              .returning[Long]
              .iterator()
        assert(it.size == 1)
      )

end entityCreatorTests
