import com.augustnagro.magnum.pg.JsonDbCodec
import play.api.libs.json.{Json, OFormat}

import java.time.LocalDate

case class LastService(mechanic: String, date: LocalDate)
    derives PlayJsonDbCodec

object LastService:
  given jsonCodec: OFormat[LastService] = Json.format[LastService]
