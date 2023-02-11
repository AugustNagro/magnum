package com.augustnagro.magnum

/** Converts camelCase scala names to snake_case */
object CamelToSnakeCase extends SqlNameMapper:

  def toColumnName(scalaName: String): String = toCase(scalaName)

  def toTableName(scalaName: String): String = toCase(scalaName)

  private def toCase(scalaName: String): String =
    val res = StringBuilder().append(scalaName.head.toLower)
    for i <- 1 until scalaName.length do
      val c = scalaName.charAt(i)
      if c.isUpper then res.append('_').append(c.toLower)
      else res.append(c)
    res.result()
