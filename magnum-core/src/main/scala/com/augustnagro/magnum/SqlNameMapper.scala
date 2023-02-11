package com.augustnagro.magnum

/** Mapping from scala terms to sql terms */
trait SqlNameMapper:
  def toColumnName(scalaName: String): String
  def toTableName(scalaName: String): String
