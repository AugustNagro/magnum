import com.augustnagro.magnum.pg.json.JsonBDbCodec
import play.api.libs.json.OFormat
import play.api.libs.json.Json

trait PlayJsonBDbCodec[A] extends JsonBDbCodec[A]

object PlayJsonBDbCodec:

  def derived[A](using jsonCodec: OFormat[A]): PlayJsonBDbCodec[A] = new:
    def encode(a: A): String = jsonCodec.writes(a).toString
    def decode(json: String): A = jsonCodec.reads(Json.parse(json)).get
