import com.augustnagro.magnum.pg.json.JsonDbCodec
import io.circe.{Codec, Decoder, Encoder, JsonObject}
import io.circe.parser.{decode as circeDecode, *}
import io.circe.syntax.*

trait CirceJsonDbCodec[A] extends JsonDbCodec[A]

object CirceJsonDbCodec:

  def derived[A: Encoder: Decoder]: CirceJsonDbCodec[A] = new:
    def encode(a: A): String = a.asJson.toString
    def decode(json: String): A = circeDecode[A](json) match
      case Right(a)  => a
      case Left(err) => throw err
