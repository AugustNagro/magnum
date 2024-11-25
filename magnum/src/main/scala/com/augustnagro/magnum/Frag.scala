package com.augustnagro.magnum

import java.lang.System.Logger.Level
import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Using}

/** Sql fragment */
class Frag(
    val sqlString: String,
    val params: Seq[Any],
    val writer: FragWriter
):
  def query[E](using reader: DbCodec[E]): Query[E] = Query(this, reader)

  def update: Update = Update(this)

  /** For databases like Postgres that support RETURNING statements via
    * `getResultSet`
    */
  def returning[E](using reader: DbCodec[E]): Returning[E] =
    Returning(this, reader, Vector.empty)

  /** For databases that support RETURNING statements via `getGeneratedKeys`
    */
  def returningKeys[E](colName: String, xs: String*)(using
      reader: DbCodec[E]
  ): Returning[E] =
    Returning(this, reader, colName +: xs)

  /** For databases that support RETURNING statements via `getGeneratedKeys`
    */
  def returningKeys[E](colName: ColumnName, xs: ColumnName*)(using
      reader: DbCodec[E]
  ): Returning[E] =
    Returning(this, reader, (colName +: xs).map(_.queryRepr))

  /** For databases that support RETURNING statements via `getGeneratedKeys`
    */
  def returningKeys[E](colNames: ColumnNames)(using
      reader: DbCodec[E]
  ): Returning[E] =
    Returning(this, reader, colNames.columnNames.map(_.queryRepr))

end Frag
