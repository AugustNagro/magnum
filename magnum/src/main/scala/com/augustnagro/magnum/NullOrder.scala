package com.augustnagro.magnum

trait NullOrder:
  def sql: String

object NullOrder:
  val First: NullOrder = new:
    def sql: String = "NULLS FIRST"

  val Last: NullOrder = new:
    def sql: String = "NULLS LAST"
