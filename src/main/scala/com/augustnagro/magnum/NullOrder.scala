package com.augustnagro.magnum

trait NullOrder:
  def sql: String

object NullOrder:
  object First extends NullOrder:
    def sql: String = "NULLS FIRST"

  object Last extends NullOrder:
    def sql: String = "NULLS LAST"
