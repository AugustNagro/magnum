import play.api.libs.json.{Json, OFormat}

case class MyJsonB(a: Vector[Int], b: String) derives PlayJsonBDbCodec

object MyJsonB:
  given jsonCodec: OFormat[MyJsonB] = Json.format[MyJsonB]
