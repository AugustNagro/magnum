import com.augustnagro.magnum.*
import munit.FunSuite

class SpecTests extends FunSuite:

  case class User(id: Long, name: String, age: opaques.Age) derives DbCodec

  test("select all"):
    assertEquals(Spec[User].build.sqlString, "")

  test("empty predicate"):
    assertEquals(Spec[User].where(sql"").build.sqlString, "")

  test("predicate having param at end"):
    val age = 3
    val frag = Spec[User]
      .where(sql"age > $age")
      .build
    assertEquals(frag.sqlString, "WHERE (age > ?)")
    assertEquals(frag.params, Vector(age))

  test("predicate having param at start"):
    val age = 3
    val spec = Spec[User]
      .where(sql"$age < age")
      .build
    assertEquals(spec.sqlString, "WHERE (? < age)")
    assertEquals(spec.params, Vector(age))

  test("AND in where predicate"):
    val name = "AUGUST"
    val age = 3
    val spec = Spec[User]
      .where(sql"age > $age AND $name = upper(name)")
      .build
    assertEquals(
      spec.sqlString,
      "WHERE (age > ? AND ? = upper(name))"
    )
    assertEquals(spec.params, Vector(age, name))

  test("multiple where predicates"):
    val name = "AUGUST"
    val age = 3
    val spec = Spec[User]
      .where(sql"age > $age")
      .where(sql"$name = upper(name)")
      .build
    assertEquals(
      spec.sqlString,
      "WHERE (age > ?) AND (? = upper(name))"
    )
    assertEquals(spec.params, Vector(age, name))

  test("orderBy"):
    val spec = Spec[User]
      .orderBy("name", SortOrder.Asc, NullOrder.Last)
      .build
    assertEquals(spec.sqlString, "ORDER BY name ASC NULLS LAST")
    assertEquals(spec.params, Vector.empty)

  test("limit"):
    val spec = Spec[User]
      .limit(99)
      .build
    assertEquals(spec.sqlString, "LIMIT 99")
    assertEquals(spec.params, Vector.empty)

  test("offset"):
    val spec = Spec[User]
      .offset(100)
      .build
    assertEquals(spec.sqlString, "OFFSET 100")
    assertEquals(spec.params, Vector.empty)

  test("seek"):
    val age = 3
    val spec = Spec[User]
      .seek("age", SeekDir.Gt, age, SortOrder.Asc)
      .build
    assertEquals(
      spec.sqlString,
      "WHERE (age > ?) ORDER BY age ASC NULLS LAST"
    )
    assertEquals(spec.params, Vector(age))

  test("seek multiple"):
    val age = 3
    val name = "John"
    val spec = Spec[User]
      .seek("age", SeekDir.Gt, age, SortOrder.Asc)
      .seek("name", SeekDir.Lt, name, SortOrder.Desc, NullOrder.First)
      .build
    assertEquals(
      spec.sqlString,
      "WHERE (age > ?) AND (name < ?) ORDER BY age ASC NULLS LAST, name DESC NULLS FIRST"
    )
    assertEquals(spec.params, Vector(age, name))

  test("everything"):
    val age = 3
    val name = "John"
    val spec = Spec[User]
      .where(sql"age > $age")
      .orderBy("age")
      .limit(10)
      .seek("name", SeekDir.Lt, name, SortOrder.Desc)
      .build
    assertEquals(
      spec.sqlString,
      "WHERE (age > ?) AND (name < ?) ORDER BY age ASC NULLS LAST, name DESC NULLS LAST LIMIT 10"
    )
    assertEquals(spec.params, Vector(age, name))
