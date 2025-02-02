import com.augustnagro.magnum.*
import munit.FunSuite

class EffectiveSubsetTests extends FunSuite:

  test("DbSchema macro error if EC not an effective subset of E"):
    case class PersonCreator(first: String, last: String)
    case class Person(id: Long, last: String) derives DbCodec
    compileErrors("DbSchema[PersonCreator, Person, Long]")

  test("Repo macro error if EC not an effective subset of E"):
    case class PersonCreator(first: String, last: String)
    case class Person(id: Long, last: String) derives DbCodec
    compileErrors("Repo[PersonCreator, Person, Long]")
