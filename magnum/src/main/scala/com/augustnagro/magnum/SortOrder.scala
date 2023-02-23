package com.augustnagro.magnum

trait SortOrder:
  def sql: String

object SortOrder:
  val Asc: SortOrder = new:
    def sql: String = "ASC"
    
  val Desc: SortOrder = new:
    def sql: String = "DESC"
