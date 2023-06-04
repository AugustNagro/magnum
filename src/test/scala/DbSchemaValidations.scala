import com.augustnagro.magnum.*
import munit.FunSuite

class DbSchemaValidations /* extends FunSuite:

  test("DbSchema macro error if EC not an effective subset of E"):
    case class PersonCreator(first: String, last: String)
    case class Person(id: Long, last: String) derives DbCodec
    compileErrors("DbSchema[PersonCreator, Person, Long]()")
*/