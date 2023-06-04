package com.augustnagro.magnum

import java.sql.PreparedStatement
import java.util.StringJoiner

class Spec[E] private (
    predicates: List[Frag],
    limit: Option[Int],
    offset: Option[Int],
    sorts: List[Sort]
):

  def where(sql: Frag): Spec[E] =
    new Spec(sql :: predicates, limit, offset, sorts)

  def orderBy(
      column: String,
      direction: SortOrder = SortOrder.Asc,
      nullOrder: NullOrder = NullOrder.Last
  ): Spec[E] =
    val sort = Sort(column, direction, nullOrder)
    new Spec(predicates, limit, offset, sort :: sorts)

  def limit(limit: Int): Spec[E] =
    new Spec(predicates, Some(limit), offset, sorts)

  def offset(offset: Int): Spec[E] =
    new Spec(predicates, limit, Some(offset), sorts)

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
    new Spec(pred :: predicates, limit, offset, sort :: sorts)

  def build: Frag =
    val whereClause = StringJoiner(" AND ", "WHERE ", "").setEmptyValue("")
    val allParams = Vector.newBuilder[Any]

    val validFrags = predicates.reverse.filter(_.sqlString.nonEmpty)
    for Frag(query, params, _) <- validFrags do
      whereClause.add("(" + query + ")")
      allParams ++= params

    val orderByClause = StringJoiner(", ", "ORDER BY ", "").setEmptyValue("")
    for Sort(col, dir, nullOrder) <- sorts.reverse do
      orderByClause.add(col + " " + dir.sql + " " + nullOrder.sql)

    val finalSj = StringJoiner(" ")
    val whereClauseStr = whereClause.toString
    if whereClauseStr.nonEmpty then finalSj.add(whereClauseStr)
    val orderByClauseStr = orderByClause.toString
    if orderByClauseStr.nonEmpty then finalSj.add(orderByClauseStr)
    for l <- limit do finalSj.add("LIMIT " + l)
    for o <- offset do finalSj.add("OFFSET " + o)

    val fragWriter: FragWriter = (ps: PreparedStatement, startingPos: Int) =>
      validFrags.foldLeft(startingPos)((pos, frag) =>
        pos + frag.writer.write(ps, pos)
      )

    Frag(finalSj.toString, allParams.result(), fragWriter)

object Spec:
  def apply[E]: Spec[E] =
    new Spec(Nil, None, None, Nil)
