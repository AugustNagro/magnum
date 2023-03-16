package com.augustnagro.magnum

import scala.reflect.ClassTag
import scala.deriving.Mirror

/** Factory for Repos */
trait DbType:
  def buildDbSchema[EC, E, ID, RES](
      tableNameSql: String,
      fieldNames: List[String],
      ecFieldNames: List[String],
      sqlNameMapper: SqlNameMapper,
      idIndex: Int
  )(using
      dbReader: DbReader[E],
      ecClassTag: ClassTag[EC],
      eClassTag: ClassTag[E],
      idClassTag: ClassTag[ID],
      eMirror: Mirror.ProductOf[E]
  ): RES
