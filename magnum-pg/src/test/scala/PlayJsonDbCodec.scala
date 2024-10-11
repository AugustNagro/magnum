import com.augustnagro.magnum.pg.JsonDbCodec
import play.api.libs.json.OFormat
import play.api.libs.json.Json

trait PlayJsonDbCodec[A] extends JsonDbCodec[A]

object PlayJsonDbCodec:

  def derived[A](using jsonCodec: OFormat[A]): PlayJsonDbCodec[A] = new:
    def encode(a: A): String = jsonCodec.writes(a).toString
    def decode(json: String): A = jsonCodec.reads(Json.parse(json)).get
