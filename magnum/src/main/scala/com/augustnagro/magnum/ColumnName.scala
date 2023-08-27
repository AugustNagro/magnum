package com.augustnagro.magnum

/** Represents an entity column. Can be interpolated in sql"" expressions */
class ColumnName(
    val scalaName: String,
    val sqlName: String,
    val queryRepr: String
) extends SqlLiteral
