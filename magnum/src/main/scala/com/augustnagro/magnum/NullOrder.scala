package com.augustnagro.magnum

trait NullOrder

object NullOrder:
  case object Default extends NullOrder
  case object First extends NullOrder
  case object Last extends NullOrder
