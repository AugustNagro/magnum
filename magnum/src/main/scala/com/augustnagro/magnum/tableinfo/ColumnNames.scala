package com.augustnagro.magnum.tableinfo

import com.augustnagro.magnum.builders.SqlLiteral
import com.augustnagro.magnum.tableinfo.ColumnName

/** A grouping of schema names, which may be interpolated in sql"" expressions.
  *
  * @param queryRepr
  *   The query representation. For example, "myColA, myColB"
  * @param columnNames
  *   The column names.
  */
class ColumnNames(val queryRepr: String, val columnNames: IArray[ColumnName])
    extends SqlLiteral
