import com.augustnagro.magnum.*
import munit.FunSuite

class SpecTests extends FunSuite:

  case class User(id: Long, name: String, age: Int)

  val user = DbSchema[User, User, Long]()

  val selectPart = "SELECT * FROM User "

  test("select all") {
    assertEquals(Spec(user).build.query, selectPart)
  }

  test("empty predicate") {
    assertEquals(Spec(user).where(sql"").build.query, selectPart)
  }

  test("predicate having param at end") {
    val age = 3
    val spec = Spec(user)
      .where(sql"${user.age} > $age")
      .build
    assertEquals(spec.query, selectPart + "WHERE (age > ?)")
    assertEquals(spec.params, Vector(age))
  }

  test("predicate having param at start") {
    val age = 3
    val spec = Spec(user)
      .where(sql"$age < ${user.age}")
      .build
    assertEquals(spec.query, selectPart + "WHERE (? < age)")
    assertEquals(spec.params, Vector(age))
  }

  test("AND in where predicate") {
    val name = "AUGUST"
    val age = 3
    val spec = Spec(user)
      .where(sql"${user.age} > $age AND $name = upper(${user.name})")
      .build
    assertEquals(spec.query, selectPart + "WHERE (age > ? AND ? = upper(name))")
    assertEquals(spec.params, Vector(age, name))
  }

  test("multiple where predicates") {
    val name = "AUGUST"
    val age = 3
    val spec = Spec(user)
      .where(sql"${user.age} > $age")
      .where(sql"$name = upper(${user.name})")
      .build
    assertEquals(
      spec.query,
      selectPart + "WHERE (age > ?) AND (? = upper(name))"
    )
    assertEquals(spec.params, Vector(age, name))
  }

  test("orderBy") {
    val spec = Spec(user)
      .orderBy(user.name, SortOrder.Asc, NullOrder.Last)
      .build
    assertEquals(spec.query, selectPart + "ORDER BY name ASC NULLS LAST")
    assertEquals(spec.params, Vector.empty)
  }

  test("limit") {
    val spec = Spec(user)
      .limit(99)
      .build
    assertEquals(spec.query, selectPart + "LIMIT 99")
    assertEquals(spec.params, Vector.empty)
  }

  test("offset") {
    val spec = Spec(user)
      .offset(100)
      .build
    assertEquals(spec.query, selectPart + "OFFSET 100")
    assertEquals(spec.params, Vector.empty)
  }

  test("seek") {
    val age = 3
    val spec = Spec(user)
      .seek(user.age, SeekDir.Gt, age, SortOrder.Asc)
      .build
    assertEquals(
      spec.query,
      selectPart + "WHERE (age > ?) ORDER BY age ASC NULLS LAST"
    )
    assertEquals(spec.params, Vector(age))
  }

  test("seek multiple") {
    val age = 3
    val name = "John"
    val spec = Spec(user)
      .seek(user.age, SeekDir.Gt, age, SortOrder.Asc)
      .seek(user.name, SeekDir.Lt, name, SortOrder.Desc, NullOrder.First)
      .build
    assertEquals(
      spec.query,
      selectPart + "WHERE (age > ?) AND (name < ?) ORDER BY age ASC NULLS LAST, name DESC NULLS FIRST"
    )
    assertEquals(spec.params, Vector(age, name))
  }

  test("everything") {
    val age = 3
    val name = "John"
    val spec = Spec(user)
      .where(sql"${user.age} > $age")
      .orderBy(user.age)
      .limit(10)
      .seek(user.name, SeekDir.Lt, name, SortOrder.Desc)
      .build
    assertEquals(
      spec.query,
      selectPart + "WHERE (age > ?) AND (name < ?) ORDER BY age ASC NULLS LAST, name DESC NULLS LAST LIMIT 10"
    )
    assertEquals(spec.params, Vector(age, name))

  }

  test("aliased schema") {
    val age = 3
    val name = "John"
    val u = user.alias("u")
    val spec = Spec(u)
      .where(sql"${u.age} > $age")
      .orderBy(u.age)
      .limit(10)
      .seek(u.name, SeekDir.Lt, name, SortOrder.Desc)
      .build
    assertEquals(
      spec.query,
      "SELECT * FROM User u WHERE (u.age > ?) AND (u.name < ?) ORDER BY u.age ASC NULLS LAST, u.name DESC NULLS LAST LIMIT 10"
    )
    assertEquals(spec.params, Vector(age, name))
  }
