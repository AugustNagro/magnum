package com.augustnagro.magnum

import scala.deriving.*
import scala.compiletime.*
import scala.quoted.*

/** Metadata about a Table, which can be interpolated in sql"" expressions
  *
  * For example,
  *
  * {{{
  *   @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  *   case class User(@Id id: Long, firstName: String)
  *     derives DbCodec
  *
  *   val u = TableInfo[User, User, Long].alias("u")
  *
  *   sql"SELECT ${u.firstName} FROM $u".sqlString ==
  *     "SELECT u.first_name FROM user u"
  * }}}
  */
class TableInfo[EC, E, ID](
    val all: ColumnNames,
    val insertColumns: ColumnNames,
    val alias: Option[String],
    val queryRepr: String,
    val idColumns: ColumnNames,
    private[magnum] val table: String,
    private[magnum] val eClassName: String
) extends Selectable, SqlLiteral:

  type Fields = NamedTuple.Map[NamedTuple.From[E], [T] =>> ColumnName]

  def selectDynamic(scalaName: String): ColumnName =
    all.columnNames.find(_.scalaName == scalaName).get

  def alias(tableAlias: String): this.type =
    require(tableAlias.nonEmpty, "custom tableAlias cannot be empty")
    val queryRepr = table + " " + tableAlias

    new TableInfo[EC, E, ID](
      all = aliasColumnNames(all, tableAlias),
      insertColumns = insertColumns,
      alias = Some(tableAlias),
      queryRepr = queryRepr,
      idColumns = aliasColumnNames(idColumns, tableAlias),
      table = table,
      eClassName = eClassName
    ).asInstanceOf[this.type]

  private def aliasColumnNames(
      columnNames: ColumnNames,
      tableAlias: String
  ): ColumnNames =
    val aliasedColumnNames = columnNames.columnNames.map(cn =>
      val sqlName = cn.sqlName
      ColumnName(
        scalaName = cn.scalaName,
        sqlName = sqlName,
        queryRepr = tableAlias + "." + sqlName
      )
    )
    val aliasedQueryRep = aliasedColumnNames.map(_.queryRepr).mkString(", ")
    ColumnNames(aliasedQueryRep, aliasedColumnNames)

end TableInfo

object TableInfo:
  inline def apply[EC: Mirror.Of, E: Mirror.Of, ID]: TableInfo[EC, E, ID] =
    ${ dbSchemaImpl[EC, E, ID] }

  @scala.annotation.publicInBinary
  private[magnum] def dbSchemaImpl[EC: Type, E: Type, ID: Type](using
      Quotes
  ): Expr[TableInfo[EC, E, ID]] =
    import quotes.reflect.*
    val exprs = tableExprs[EC, E, ID]

    val allColumnsExpr = Expr.ofSeq(
      exprs.eElemNames
        .lazyZip(exprs.eElemNamesSql)
        .map((elemName, elemNameSqlExpr) =>
          '{
            val elemNameSql = $elemNameSqlExpr
            ColumnName(${ Expr(elemName) }, elemNameSql, elemNameSql)
          }
        )
    )

    val insertColumnsExpr = Expr.ofSeq(
      exprs.ecElemNames
        .lazyZip(exprs.ecElemNamesSql)
        .map((elemName, elemNameSqlExpr) =>
          '{
            val elemNameSql = $elemNameSqlExpr
            ColumnName(${ Expr(elemName) }, elemNameSql, elemNameSql)
          }
        )
    )

    '{
      val allColumns = IArray.from($allColumnsExpr)
      val allQueryRepr = allColumns.map(_.queryRepr).mkString(", ")
      val allCols = ColumnNames(allQueryRepr, allColumns)

      val insertColumns = IArray.from($insertColumnsExpr)
      val insertQueryRepr =
        insertColumns.map(_.queryRepr).mkString("(", ", ", ")")
      val insertCols = ColumnNames(insertQueryRepr, insertColumns)

      val idIndices = IArray.from(${ exprs.idIndices })
      val idColumns = idIndices.map(allColumns.apply)
      val idColumnsQueryRepr = idColumns.map(_.queryRepr).mkString(", ")
      val idColumnNames = ColumnNames(idColumnsQueryRepr, idColumns)

      val tableName = ${ exprs.tableNameSql }
      new TableInfo[EC, E, ID](
        all = allCols,
        insertColumns = insertCols,
        alias = None,
        table = tableName,
        queryRepr = tableName,
        idColumns = idColumnNames,
        eClassName = ${ exprs.tableNameScala }
      )
    }
  end dbSchemaImpl
end TableInfo
