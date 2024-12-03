package com.augustnagro.magnum.spec

trait SortOrder

object SortOrder:
  case object Default extends SortOrder
  case object Asc extends SortOrder
  case object Desc extends SortOrder
