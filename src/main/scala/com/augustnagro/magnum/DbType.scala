package com.augustnagro.magnum

import scala.reflect.ClassTag
import scala.deriving.Mirror

/** Factory for Repo default methods */
trait DbType:
  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      fieldNames: List[String],
      fieldNamesSql: List[String],
      ecFieldNames: List[String],
      ecFieldNamesSql: List[String],
      idIndex: Int
  )(using
      dbCodec: DbCodec[E],
      ecClassTag: ClassTag[EC],
      eClassTag: ClassTag[E],
      idClassTag: ClassTag[ID],
      eMirror: Mirror.ProductOf[E]
  ): RepoDefaults[EC, E, ID]
