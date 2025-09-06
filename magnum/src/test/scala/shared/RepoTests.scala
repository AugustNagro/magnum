package shared

import com.augustnagro.magnum.*
import com.augustnagro.magnum.{BatchUpdateResult, SqlException, ColumnNames}
import munit.FunSuite

import java.time.OffsetDateTime
import java.util.UUID

def repoTests(suite: FunSuite, dbType: DbType, xa: () => Transactor)(using
    munit.Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime,
      socialId: Option[UUID]
  ) derives DbCodec

  val personRepo = Repo[Person, Person, Long]
  val person = TableInfo[Person, Person, Long]

  test("delete"):
    xa().connect:
      val p = personRepo.findById(1L).get
      personRepo.delete(p)
      assert(personRepo.findById(1L) == None)

  test("delete invalid"):
    xa().connect:
      personRepo.delete(
        Person(999L, None, "", false, OffsetDateTime.now, None)
      )
      assert(8L == personRepo.count)

  test("deleteById"):
    xa().connect:
      personRepo.deleteById(1L)
      personRepo.deleteById(2L)
      personRepo.deleteById(1L)
      assert(personRepo.findAll.size == 6)

  test("deleteAll"):
    xa().connect:
      val p1 = personRepo.findById(1L).get
      val p2 = p1.copy(id = 2L)
      val p3 = p1.copy(id = 999L)
      val expectedRowsUpdate = dbType match
        case ClickhouseDbType => 3
        case _                => 2
      val res = personRepo.deleteAll(Vector(p1, p2, p3))
      assert(res == BatchUpdateResult.Success(expectedRowsUpdate))
      assert(6L == personRepo.count)

  test("deleteAllById"):
    xa().connect:
      val expectedRowsUpdate = dbType match
        case ClickhouseDbType => 3
        case _                => 2
      val res = personRepo.deleteAllById(Vector(1L, 2L, 1L))
      assert(res == BatchUpdateResult.Success(expectedRowsUpdate))
      assert(6L == personRepo.count)

  test("truncate"):
    xa().connect:
      personRepo.truncate()
      assert(personRepo.count == 0L)

  test("insert"):
    xa().connect:
      personRepo.insert(
        Person(
          id = 9L,
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          socialId = Some(UUID.randomUUID),
          created = OffsetDateTime.now
        )
      )
      personRepo.insert(
        Person(
          id = 10L,
          firstName = None,
          lastName = "Prince",
          isAdmin = true,
          socialId = None,
          created = OffsetDateTime.now
        )
      )
      assert(personRepo.count == 10L)
      assert(personRepo.findAll.map(_.lastName).contains("Smith"))

  test("insertReturning"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val person = personRepo.insertReturning(
        Person(
          id = 9L,
          firstName = Some("John"),
          lastName = "Smith",
          isAdmin = false,
          socialId = None,
          created = OffsetDateTime.now
        )
      )
      assert(person.lastName == "Smith")

  test("insertAllReturning"):
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val newPc = Vector(
        Person(
          id = 9L,
          firstName = Some("Chandler"),
          lastName = "Johnsored",
          isAdmin = true,
          socialId = Some(UUID.randomUUID()),
          created = OffsetDateTime.now
        ),
        Person(
          id = 10L,
          firstName = None,
          lastName = "Odysseus",
          isAdmin = false,
          socialId = None,
          created = OffsetDateTime.now
        ),
        Person(
          id = 11L,
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = true,
          socialId = None,
          created = OffsetDateTime.now
        )
      )
      val people = personRepo.insertAllReturning(newPc)
      assert(personRepo.count == 11L)
      assert(people.size == 3)
      assert(people.last.lastName == newPc.last.lastName)

  test("insert invalid"):
    intercept[SqlException]:
      xa().connect:
        val invalidP =
          Person(9L, None, null, false, OffsetDateTime.now, None)
        personRepo.insert(invalidP)

  test("update"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val p = personRepo.findById(1L).get
      val updated = p.copy(firstName = None, isAdmin = false)
      personRepo.update(updated)
      assert(personRepo.findById(1L).get == updated)

  test("update invalid"):
    assume(dbType != ClickhouseDbType)
    intercept[SqlException]:
      xa().connect:
        val p = personRepo.findById(1L).get
        val updated = p.copy(lastName = null)
        personRepo.update(updated)

  test("insertAll"):
    xa().connect:
      val newPeople = Vector(
        Person(
          id = 9L,
          firstName = Some("Chandler"),
          lastName = "Johnsored",
          isAdmin = true,
          socialId = Some(UUID.randomUUID()),
          created = OffsetDateTime.now
        ),
        Person(
          id = 10L,
          firstName = None,
          lastName = "Odysseus",
          isAdmin = false,
          socialId = None,
          created = OffsetDateTime.now
        ),
        Person(
          id = 11L,
          firstName = Some("Jorge"),
          lastName = "Masvidal",
          isAdmin = true,
          socialId = None,
          created = OffsetDateTime.now
        )
      )
      personRepo.insertAll(newPeople)
      assert(personRepo.count == 11L)
      assert(
        personRepo.findAll.map(_.lastName).contains(newPeople.last.lastName)
      )

  test("updateAll"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val newPeople = Vector(
        personRepo.findById(1L).get.copy(lastName = "Peterson"),
        personRepo.findById(2L).get.copy(lastName = "Moreno")
      )
      val res = personRepo.updateAll(newPeople)
      assert(res == BatchUpdateResult.Success(2))
      assert(personRepo.findById(1L).get == newPeople(0))
      assert(personRepo.findById(2L).get == newPeople(1))

  test("transact"):
    assume(dbType != ClickhouseDbType)
    val count = xa().transact:
      val p = Person(
        id = 9L,
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false,
        created = OffsetDateTime.now,
        socialId = None
      )
      personRepo.insert(p)
      personRepo.count
    assert(count == 9L)

  test("transact failed"):
    assume(dbType != ClickhouseDbType)
    val transactor = xa()
    val p = Person(
      id = 9L,
      firstName = Some("Chandler"),
      lastName = "Brown",
      isAdmin = false,
      created = OffsetDateTime.now,
      socialId = None
    )
    try
      transactor.transact:
        personRepo.insert(p)
        throw RuntimeException()
      fail("should not reach")
    catch
      case _: Exception =>
        transactor.transact:
          assert(personRepo.count == 8L)

  test("custom insert"):
    xa().connect:
      val p = Person(
        id = 9L,
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false,
        socialId = None,
        created = OffsetDateTime.now
      )
      val update =
        sql"insert into $person ${person.insertColumns} values ($p)".update
      assertNoDiff(
        update.frag.sqlString,
        "insert into person (id, first_name, last_name, is_admin, created, social_id) values (?, ?, ?, ?, ?, ?)"
      )
      val rowsInserted = update.run()
      assert(rowsInserted == 1)
      assert(personRepo.count == 9L)
      assert(
        personRepo.findAll.exists(fetched =>
          fetched.firstName == p.firstName &&
            fetched.lastName == p.lastName &&
            fetched.isAdmin == p.isAdmin
        )
      )

  test("custom update"):
    xa().connect:
      val p = Person(
        id = 9L,
        firstName = Some("Chandler"),
        lastName = "Brown",
        isAdmin = false,
        socialId = Some(UUID.randomUUID()),
        created = OffsetDateTime.now
      )
      personRepo.insert(p)
      val newIsAdmin = true
      val update =
        sql"update $person set ${person.isAdmin} = $newIsAdmin where ${person.id} = ${p.id}".update
      assertNoDiff(
        update.frag.sqlString,
        "update person set person.is_admin = ? where person.id = ?"
      )
      val rowsUpdated = update.run()
      assert(rowsUpdated == 1)
      assert(personRepo.findById(p.id).get.isAdmin == true)

  test("custom returning a single column"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val personId =
        if dbType == H2DbType then
          sql"""insert into person (id, first_name, last_name, created, is_admin)
                values (9, 'Arton', 'Senna', now(), true)
                """
            .returningKeys[Long]("id")
            .run()
            .head
        else if dbType == OracleDbType then
          sql"""insert into person (id, first_name, last_name, created, is_admin)
                values (9, 'Arton', 'Senna', current_timestamp, 'Y')"""
            .returningKeys[Long]("id")
            .run()
            .head
        else
          sql"""insert into person (id, first_name, last_name, created, is_admin)
                values (9, 'Arton', 'Senna', now(), 'Y') RETURNING id
                """.returning[Long].run().head
      assert(personRepo.findById(personId).get.lastName == "Senna")

  test("custom returning multiple columns"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    assume(dbType != OracleDbType)
    xa().connect:
      val cols =
        if dbType == H2DbType then
          sql"""insert into person (id, first_name, last_name, created, is_admin) values
             (9, 'Arton', 'Senna', now(), true),
             (10, 'Demo', 'User', now(), false)
             """
            .returningKeys[(Long, OffsetDateTime)](
              person.id,
              person.created
            )
            .run()
        else
          sql"""insert into person (id, first_name, last_name, created, is_admin) values
               (9, 'Arton', 'Senna', now(), true),
               (10, 'Demo', 'User', now(), false)
               RETURNING id, created
               """.returning[(Long, OffsetDateTime)].run()
      val newLastNames =
        cols.map((id, _) => personRepo.findById(id).get.lastName)
      assert(newLastNames == Vector("Senna", "User"))

  test("custom returning with no rows updated"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val personIds =
        if dbType == H2DbType || dbType == OracleDbType then
          sql"update person set first_name = 'xxx' where last_name = 'Not Here'"
            .returningKeys[Long](ColumnNames("id", IArray(person.id)))
            .run()
        else
          sql"update person set first_name = 'xxx' where last_name = 'Not Here' returning id"
            .returning[Long]
            .run()
      assert(personIds.isEmpty)

  test("returning non primary key column"):
    assume(dbType != ClickhouseDbType)
    assume(dbType != MySqlDbType)
    assume(dbType != SqliteDbType)
    xa().connect:
      val personFirstNames =
        if dbType == H2DbType || dbType == OracleDbType then
          sql"update person set last_name = 'xxx'"
            .returningKeys[String](person.firstName)
            .run()
        else
          sql"update person set last_name = 'xxx' returning first_name"
            .returning[String]
            .run()

      assert(personFirstNames.nonEmpty)
end repoTests
