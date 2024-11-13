import com.augustnagro.magnum.pg.json.JsonDbCodec

import io.circe.Codec
import java.time.LocalDate

case class LastService(mechanic: String, date: LocalDate)
    derives Codec.AsObject, CirceJsonDbCodec
