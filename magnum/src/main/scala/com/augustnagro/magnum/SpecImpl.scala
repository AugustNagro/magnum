package com.augustnagro.magnum

import java.util.StringJoiner

private trait SpecImpl:
  def sortSql(sort: Sort): String =
    val dir = sort.direction match
      case SortOrder.Default => ""
      case SortOrder.Asc     => " ASC"
      case SortOrder.Desc    => " DESC"

    val nullOrder = sort.nullOrder match
      case NullOrder.Default => ""
      case NullOrder.First   => " NULLS FIRST"
      case NullOrder.Last    => " NULLS LAST"

    sort.column + dir + nullOrder

  def offsetLimitSql(offset: Option[Long], limit: Option[Int]): Option[String] =
    (offset, limit) match
      case (Some(o), Some(l)) => Some(s"OFFSET $o LIMIT $l")
      case (Some(o), None)    => Some(s"OFFSET $o")
      case (None, Some(l))    => Some(s"LIMIT $l")
      case (None, None)       => None

  def orderBy(sorts: Vector[Sort]): String =
    val orderByClause =
      StringJoiner(", ", "ORDER BY ", "").setEmptyValue("")
    for sort <- sorts do orderByClause.add(sortSql(sort))
    orderByClause.toString

  def seekSql(seek: Seek): String =
    val seekDir = seek.seekDirection match
      case SeekDir.Gt => ">"
      case SeekDir.Lt => "<"

    s"${seek.column} $seekDir ?"

  def findAll[E: DbCodec](spec: Spec[E], tableNameSql: String)(using
      DbCon
  ): Vector[E] =
    val whereClause = StringJoiner(" AND ", "WHERE ", "").setEmptyValue("")

    val allParams = Vector.newBuilder[Any]

    val tableNameLiteral = SqlLiteral(tableNameSql)
    val prefixFrag = spec.prefix.getOrElse(sql"SELECT * FROM $tableNameLiteral")
    allParams ++= prefixFrag.params

    val seekPredicates = spec.seeks.map(seek =>
      val codec = seek.codec.asInstanceOf[DbCodec[Any]]
      Frag(
        seekSql(seek),
        Vector(seek.value),
        (ps, pos) =>
          codec.writeSingle(seek.value, ps, pos)
          pos + codec.cols.length
      )
    )

    val whereFrags =
      (spec.predicates ++ seekPredicates).filter(_.sqlString.nonEmpty)
    for frag <- whereFrags do
      whereClause.add("(" + frag.sqlString + ")")
      allParams ++= frag.params

    val seekSorts =
      spec.seeks.map(seek => Sort(seek.column, seek.columnSort, seek.nullOrder))
    val sorts = spec.sorts ++ seekSorts

    val finalSj = StringJoiner(" ")
    if prefixFrag.sqlString.nonEmpty then finalSj.add(prefixFrag.sqlString)
    val whereClauseStr = whereClause.toString
    if whereClauseStr.nonEmpty then finalSj.add(whereClauseStr)
    val orderByClauseStr = orderBy(sorts)
    if orderByClauseStr.nonEmpty then finalSj.add(orderByClauseStr)
    for ol <- offsetLimitSql(spec.offset, spec.limit) do finalSj.add(ol)

    val allFrags = prefixFrag +: whereFrags
    val fragWriter: FragWriter = (ps, startingPos) =>
      allFrags.foldLeft(startingPos)((pos, frag) => frag.writer.write(ps, pos))

    Frag(finalSj.toString, allParams.result(), fragWriter)
      .query[E]
      .run()
  end findAll
end SpecImpl

private object SpecImpl:
  object Default extends SpecImpl
