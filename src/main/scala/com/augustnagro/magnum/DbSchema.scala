package com.augustnagro.magnum

import scala.deriving.*
import scala.compiletime.*
import scala.quoted.*

class DbSchema[EC, E, ID](
    val all: SchemaNames,
    val insertColumns: SchemaNames,
    val alias: String,
    private[magnum] val table: String,
    private[magnum] val tableWithAlias: String,
    private[magnum] val eClassName: String
) extends Selectable:

  def selectDynamic(scalaName: String): SchemaName =
    all.schemaNames.find(_.scalaName == scalaName).get

  def alias(tableAlias: String): this.type =
    require(tableAlias.nonEmpty, "custom tableAlias cannot be empty")
    val tableWithAlias = table + " " + tableAlias

    val allSchemaNames = all.schemaNames.map(sn =>
      sn.copy(sqlNameAliased = tableAlias + "." + sn.sqlName)
    )
    val allQueryRepr = allSchemaNames.map(_.sqlNameAliased).mkString(", ")
    val allCols = SchemaNames(allQueryRepr, allSchemaNames)

    new DbSchema[EC, E, ID](
      allCols,
      insertColumns,
      tableAlias,
      table,
      tableWithAlias,
      eClassName
    ).asInstanceOf[this.type]
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
        Refinement(typeRepr, elemName, TypeRepr.of[SchemaName])
      )

    val allSchemaNamesExpr = Expr.ofSeq(
      exprs.eElemNames
        .lazyZip(exprs.eElemNamesSql)
        .map((elemName, elemNameSqlExpr) =>
          '{
            val elemNameSql = $elemNameSqlExpr
            SchemaName(${ Expr(elemName) }, elemNameSql, elemNameSql)
          }
        )
    )

    val insertColumnsExpr = Expr.ofSeq(
      exprs.ecElemNames
        .lazyZip(exprs.ecElemNamesSql)
        .map((elemName, elemNameSqlExpr) =>
          '{
            val elemNameSql = $elemNameSqlExpr
            SchemaName(${ Expr(elemName) }, elemNameSql, elemNameSql)
          }
        )
    )

    refinement.asType match
      case '[tpe] =>
        '{
          val allSchemaNames = IArray.from($allSchemaNamesExpr)
          val allQueryRepr = allSchemaNames.map(_.sqlNameAliased).mkString(", ")
          val allCols = SchemaNames(allQueryRepr, allSchemaNames)

          val insertSchemaNames = IArray.from($insertColumnsExpr)
          val insertQueryRepr =
            insertSchemaNames.map(_.sqlNameAliased).mkString("(", ", ", ")")
          val insertCols = SchemaNames(insertQueryRepr, insertSchemaNames)

          val tableName = ${ exprs.tableNameSql }
          new DbSchema[EC, E, ID](
            all = allCols,
            insertColumns = insertCols,
            alias = defaultAlias,
            table = tableName,
            tableWithAlias = tableName,
            eClassName = ${ exprs.tableNameScala }
          ).asInstanceOf[tpe]
        }
    end match
  end dbSchemaImpl
end DbSchema
