import io.circe.Codec

case class MyJsonB(a: Vector[Int], b: String)
    derives Codec.AsObject, CirceJsonBDbCodec
