package com.augustnagro.magnum

/** SqlNameMapper that keeps the same case as the provided scala names */
object SameCase extends SqlNameMapper:

  def toColumnName(scalaName: String): String = scalaName

  def toTableName(scalaName: String): String = scalaName
