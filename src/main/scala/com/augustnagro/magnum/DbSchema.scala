package com.augustnagro.magnum

import scala.deriving.*
import scala.compiletime.*
import scala.quoted.*

class DbSchema[EC, E, ID](
    val all: ColumnNames,
    val insertColumns: ColumnNames,
    val alias: String,
    val queryRepr: String,
    private[magnum] val table: String,
    private[magnum] val eClassName: String
) extends Selectable,
      SqlLiteral:

  def selectDynamic(scalaName: String): ColumnName =
    all.columnNames.find(_.scalaName == scalaName).get

  def alias(tableAlias: String): this.type =
    require(tableAlias.nonEmpty, "custom tableAlias cannot be empty")
    val queryRepr = table + " " + tableAlias

    val allSchemaNames = all.columnNames.map(cn =>
      val sqlName = cn.sqlName
      ColumnName(
        scalaName = cn.scalaName,
        sqlName = sqlName,
        queryRepr = tableAlias + "." + sqlName
      )
    )
    val allQueryRepr = allSchemaNames.map(_.queryRepr).mkString(", ")
    val allCols = ColumnNames(allQueryRepr, allSchemaNames)

    new DbSchema[EC, E, ID](
      all = allCols,
      insertColumns = insertColumns,
      alias = tableAlias,
      queryRepr = queryRepr,
      table = table,
      eClassName = eClassName
    ).asInstanceOf[this.type]
  end alias
end DbSchema

object DbSchema:
  val defaultAlias = ""

  transparent inline def apply[EC: Mirror.Of, E: Mirror.Of, ID] =
    ${ dbSchemaImpl[EC, E, ID] }

  private def dbSchemaImpl[EC: Type, E: Type, ID: Type](using
      Quotes
  ): Expr[Any] =
    import quotes.reflect.*
    val exprs = tableExprs[EC, E, ID]
    val refinement = exprs.eElemNames
      .foldLeft(TypeRepr.of[DbSchema[EC, E, ID]])((typeRepr, elemName) =>
        Refinement(typeRepr, elemName, TypeRepr.of[ColumnName])
      )

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

    refinement.asType match
      case '[tpe] =>
        '{
          val allColumns = IArray.from($allColumnsExpr)
          val allQueryRepr = allColumns.map(_.queryRepr).mkString(", ")
          val allCols = ColumnNames(allQueryRepr, allColumns)

          val insertColumns = IArray.from($insertColumnsExpr)
          val insertQueryRepr =
            insertColumns.map(_.queryRepr).mkString("(", ", ", ")")
          val insertCols = ColumnNames(insertQueryRepr, insertColumns)

          val tableName = ${ exprs.tableNameSql }
          new DbSchema[EC, E, ID](
            all = allCols,
            insertColumns = insertCols,
            alias = defaultAlias,
            table = tableName,
            queryRepr = tableName,
            eClassName = ${ exprs.tableNameScala }
          ).asInstanceOf[tpe]
        }
    end match
  end dbSchemaImpl
end DbSchema
