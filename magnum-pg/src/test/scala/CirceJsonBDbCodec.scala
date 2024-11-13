import com.augustnagro.magnum.pg.json.JsonBDbCodec
import io.circe.{Codec, Decoder, Encoder, JsonObject}
import io.circe.parser.{decode as circeDecode, *}
import io.circe.syntax.*

trait CirceJsonBDbCodec[A] extends JsonBDbCodec[A]

object CirceJsonBDbCodec:
  def derived[A: Encoder: Decoder]: CirceJsonBDbCodec[A] = new:
    def encode(a: A): String = a.asJson.toString
    def decode(json: String): A = circeDecode[A](json) match
      case Right(a)  => a
      case Left(err) => throw err
