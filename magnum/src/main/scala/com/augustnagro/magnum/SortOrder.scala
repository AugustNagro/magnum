package com.augustnagro.magnum

trait SortOrder

object SortOrder:
  case object Default extends SortOrder
  case object Asc extends SortOrder
  case object Desc extends SortOrder
