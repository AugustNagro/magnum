package com.augustnagro.magnum

import scala.quoted.*

private case class TableExprs(
    tableAnnot: Expr[Table],
    tableNameScala: Expr[String],
    tableNameSql: Expr[String],
    eElemNames: Seq[String],
    eElemNamesSql: Seq[Expr[String]],
    ecElemNames: List[String],
    ecElemNamesSql: Seq[Expr[String]],
    idIndex: Expr[Int]
)
