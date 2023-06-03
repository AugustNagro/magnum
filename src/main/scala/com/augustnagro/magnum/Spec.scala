package com.augustnagro.magnum

import java.util.StringJoiner

class Spec[E] private (
                        tableName: String,
                        predicates: List[Frag],
                        limit: Option[Int],
                        offset: Option[Int],
                        sorts: List[Sort]
):

  def where(sql: Frag): Spec[E] =
    new Spec(tableName, sql :: predicates, limit, offset, sorts)

  def orderBy(
      column: String,
      direction: SortOrder = SortOrder.Asc,
      nullOrder: NullOrder = NullOrder.Last
  ): Spec[E] =
    val sort = Sort(column, direction, nullOrder)
    new Spec(tableName, predicates, limit, offset, sort :: sorts)

  def limit(limit: Int): Spec[E] =
    new Spec(tableName, predicates, Some(limit), offset, sorts)

  def offset(offset: Int): Spec[E] =
    new Spec(tableName, predicates, limit, Some(offset), sorts)

  def seek(
      column: String,
      seekDirection: SeekDir,
      value: Any,
      columnSort: SortOrder,
      nullOrder: NullOrder = NullOrder.Last
  ): Spec[E] =
    val sort = Sort(column, columnSort, nullOrder)
    val pred = Frag(s"$column ${seekDirection.sql} ?", Vector(value))
    new Spec(tableName, pred :: predicates, limit, offset, sort :: sorts)

  def build: Frag =
    val whereClause = StringJoiner(" AND ", "WHERE ", "").setEmptyValue("")
    val allParams = Vector.newBuilder[Any]

    for Frag(query, params) <- predicates.reverse if query.nonEmpty do
      whereClause.add("(" + query + ")")
      allParams ++= params

    val orderByClause = StringJoiner(", ", "ORDER BY ", "").setEmptyValue("")
    for Sort(col, dir, nullOrder) <- sorts.reverse do
      orderByClause.add(col + " " + dir.sql + " " + nullOrder.sql)

    val selectPart = "SELECT * FROM " + tableName + " "
    val finalSj = StringJoiner(" ", selectPart, "")
    val whereClauseStr = whereClause.toString
    if whereClauseStr.nonEmpty then finalSj.add(whereClauseStr)
    val orderByClauseStr = orderByClause.toString
    if orderByClauseStr.nonEmpty then finalSj.add(orderByClauseStr)
    for l <- limit do finalSj.add("LIMIT " + l)
    for o <- offset do finalSj.add("OFFSET " + o)

    Frag(finalSj.toString, allParams.result())

object Spec:
  def apply[E](tableName: String): Spec[E] =
    new Spec(tableName, Nil, None, None, Nil)
