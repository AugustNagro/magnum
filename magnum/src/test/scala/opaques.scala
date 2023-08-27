import com.augustnagro.magnum.DbCodec

object opaques:
  opaque type Age = Int
  object Age:
    def apply(value: Int): Age = value
    extension (opaque: Age) def value: Int = opaque

    given DbCodec[opaques.Age] =
      DbCodec.IntCodec.biMap(opaques.Age(_), _.value)
