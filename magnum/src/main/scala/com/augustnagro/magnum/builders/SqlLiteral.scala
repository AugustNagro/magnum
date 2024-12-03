package com.augustnagro.magnum.builders

/** A SQL string that is interpolated directly into a sql"" query (and not as a
  * PreparedStatement parameter)
  *
  * For example,
  *
  * {{{
  *   val myQaSchema = SqlLiteral("db_qa")
  *   sql"SELECT * FROM $myQaSchema.table_name"
  * }}}
  *
  * Generates the SQL:
  * {{{
  *   "SELECT * FROM db_qa.table_name"
  * }}}
  */
trait SqlLiteral:
  def queryRepr: String

object SqlLiteral:
  def apply(s: String): SqlLiteral =
    new SqlLiteral:
      def queryRepr: String = s
