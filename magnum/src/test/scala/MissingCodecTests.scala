import munit.FunSuite

import scala.compiletime.testing.Error as CompileError
import scala.compiletime.testing.typeCheckErrors

class MissingCodecTests extends FunSuite:

  test("sql interpolator reports a missing DbCodec"):
    val errors: List[CompileError] = typeCheckErrors("""
      import com.augustnagro.magnum.*

      case class MissingCodec(value: String)

      sql"select ${MissingCodec("value")}".query[Long]
    """)
    assert(
      errors.exists(
        _.message.contains("Could not find given DbCodec for MissingCodec.")
      )
    )

  test("Repo reports a missing product field DbCodec"):
    val errors: List[CompileError] = typeCheckErrors("""
      import com.augustnagro.magnum.*

      @Table(H2DbType)
      case class MissingFieldCodec(@Id id: Long, value: CharSequence)

      given DbCodec[MissingFieldCodec] =
        DbCodec[(Long, String)].biMap(
          tuple => MissingFieldCodec(tuple._1, tuple._2),
          entity => (entity.id, entity.value.toString)
        )

      Repo[MissingFieldCodec, MissingFieldCodec, Long]
    """)
    assert(
      errors.exists(
        _.message.contains(
          "Could not find given DbCodec for java.lang.CharSequence."
        )
      )
    )
end MissingCodecTests
