package com.augustnagro.magnum

import java.sql.PreparedStatement
import java.util.StringJoiner

// todo PrefixSpec <: Spec ?
class Spec[E] private (
    private[magnum] val prefix: Option[Frag],
    predicates: List[Frag],
    limit: Option[Int],
    offset: Option[Int],
    sorts: List[Sort]
):

  def where(sql: Frag): Spec[E] =
    new Spec(prefix, sql :: predicates, limit, offset, sorts)

  def orderBy(
      column: String,
      direction: SortOrder = SortOrder.Asc,
      nullOrder: NullOrder = NullOrder.Last
  ): Spec[E] =
    val sort = Sort(column, direction, nullOrder)
    new Spec(prefix, predicates, limit, offset, sort :: sorts)

  def limit(limit: Int): Spec[E] =
    new Spec(prefix, predicates, Some(limit), offset, sorts)

  def offset(offset: Int): Spec[E] =
    new Spec(prefix, predicates, limit, Some(offset), sorts)

  def seek[V](
      column: String,
      seekDirection: SeekDir,
      value: V,
      columnSort: SortOrder,
      nullOrder: NullOrder = NullOrder.Last
  )(using codec: DbCodec[V]): Spec[E] =
    val sort = Sort(column, columnSort, nullOrder)
    val pred =
      Frag(
        s"$column ${seekDirection.sql} ?",
        Vector(value),
        (ps, pos) =>
          codec.writeSingle(value, ps, pos)
          pos + codec.cols.length
      )
    new Spec(prefix, pred :: predicates, limit, offset, sort :: sorts)

  def build: Frag =
    val whereClause = StringJoiner(" AND ", "WHERE ", "").setEmptyValue("")
    val allParams = Vector.newBuilder[Any]

    val prefixFrag = prefix.getOrElse(Frag(""))
    allParams ++= prefixFrag.params

    val validFrags = predicates.reverse.filter(_.sqlString.nonEmpty)
    for frag <- validFrags do
      whereClause.add("(" + frag.sqlString + ")")
      allParams ++= frag.params

    val orderByClause = StringJoiner(", ", "ORDER BY ", "").setEmptyValue("")
    for Sort(col, dir, nullOrder) <- sorts.reverse do
      orderByClause.add(col + " " + dir.sql + " " + nullOrder.sql)

    val finalSj = StringJoiner(" ")
    val prefixSql = prefixFrag.sqlString
    if prefixSql.nonEmpty then finalSj.add(prefixSql)
    val whereClauseStr = whereClause.toString
    if whereClauseStr.nonEmpty then finalSj.add(whereClauseStr)
    val orderByClauseStr = orderByClause.toString
    if orderByClauseStr.nonEmpty then finalSj.add(orderByClauseStr)
    for l <- limit do finalSj.add("LIMIT " + l)
    for o <- offset do finalSj.add("OFFSET " + o)

    val fragWriter: FragWriter = (ps, startingPos) =>
      (prefixFrag :: validFrags).foldLeft(startingPos)((pos, frag) =>
        pos + frag.writer.write(ps, pos)
      )

    Frag(finalSj.toString, allParams.result(), fragWriter)
  end build

  def query(using DbCodec[E]): Query[E] =
    build.query[E]

end Spec

object Spec:
  def apply[E]: Spec[E] =
    new Spec(None, Nil, None, None, Nil)

  /** Create a Spec with the desired prefix.
    * @param prefix
    *   a SQL fragment that ends before the WHERE clause.
    */
  def apply[E](prefix: Frag): Spec[E] =
    new Spec(Some(prefix), Nil, None, None, Nil)
