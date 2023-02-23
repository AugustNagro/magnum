package com.augustnagro.magnum

import java.util.StringJoiner

class Spec[E] private (
    schema: DbSchema[?, E, ?],
    predicates: List[Sql],
    limit: Option[Int],
    offset: Option[Int],
    sorts: List[Sort]
):

  def where(sql: Sql): Spec[E] =
    new Spec(schema, sql :: predicates, limit, offset, sorts)

  def orderBy(
      column: DbSchemaName,
      direction: SortOrder = SortOrder.Asc,
      nullOrder: NullOrder = NullOrder.Last
  ): Spec[E] =
    val sort = Sort(column, direction, nullOrder)
    new Spec(schema, predicates, limit, offset, sort :: sorts)

  def limit(limit: Int): Spec[E] =
    new Spec(schema, predicates, Some(limit), offset, sorts)

  def offset(offset: Int): Spec[E] =
    new Spec(schema, predicates, limit, Some(offset), sorts)

  def seek(
      column: DbSchemaName,
      seekDirection: SeekDir,
      value: Any,
      columnSort: SortOrder,
      nullOrder: NullOrder = NullOrder.Last
  ): Spec[E] =
    val sort = Sort(column, columnSort, nullOrder)
    val pred = Sql(s"${column.sqlName} ${seekDirection.sql} ?", Vector(value))
    new Spec(schema, pred :: predicates, limit, offset, sort :: sorts)

  def build: Sql =
    val whereClause = StringJoiner(" AND ", "WHERE ", "").setEmptyValue("")
    val allParams = Vector.newBuilder[Any]

    for Sql(query, params) <- predicates.reverse if query.nonEmpty do
      whereClause.add("(" + query + ")")
      allParams ++= params

    val orderByClause = StringJoiner(", ", "ORDER BY ", "").setEmptyValue("")
    for Sort(col, dir, nullOrder) <- sorts.reverse do
      orderByClause.add(col.sqlName + " " + dir.sql + " " + nullOrder.sql)

    val selectPart = "SELECT * FROM " + schema.tableWithAlias + " "
    val finalSj = StringJoiner(" ", selectPart, "")
    val whereClauseStr = whereClause.toString
    if whereClauseStr.nonEmpty then finalSj.add(whereClauseStr)
    val orderByClauseStr = orderByClause.toString
    if orderByClauseStr.nonEmpty then finalSj.add(orderByClauseStr)
    for l <- limit do finalSj.add("LIMIT " + l)
    for o <- offset do finalSj.add("OFFSET " + o)

    Sql(finalSj.toString, allParams.result())

object Spec:
  def apply[E](schema: DbSchema[?, E, ?]): Spec[E] =
    new Spec(schema, Nil, None, None, Nil)
