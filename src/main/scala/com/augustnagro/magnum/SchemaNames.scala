package com.augustnagro.magnum

/** A grouping of schema names, which may be interpolated in sql"" expressions.
  * @param queryRepr
  *   The query representation. For example, "myColA, myColB"
  * @param schemaNames
  *   The schema names.
  */
case class SchemaNames(queryRepr: String, schemaNames: IArray[SchemaName])
