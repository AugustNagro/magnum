package com.augustnagro.magnum

/** Mapping from scala terms to sql terms */
trait SqlNameMapper:
  def toColumnName(scalaName: String): String
  def toTableName(scalaName: String): String

object SqlNameMapper:

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
  end CamelToSnakeCase

  /** SqlNameMapper that keeps the same case as the provided scala names */
  object SameCase extends SqlNameMapper:
    def toColumnName(scalaName: String): String = scalaName
    def toTableName(scalaName: String): String = scalaName
