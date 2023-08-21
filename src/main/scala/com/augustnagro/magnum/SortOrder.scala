package com.augustnagro.magnum

trait SortOrder:
  def sql: String

object SortOrder:
  object Asc extends SortOrder:
    def sql: String = "ASC"

  object Desc extends SortOrder:
    def sql: String = "DESC"
