package shared

import com.augustnagro.magnum.codec.DbCodec

enum Color derives DbCodec:
  case Red, Green, Blue
