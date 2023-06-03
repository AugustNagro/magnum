package com.augustnagro.magnum

import scala.reflect.ClassTag
import scala.deriving.Mirror

/** Factory for Repo default methods */
trait DbType:
  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      eElemNames: Seq[String],
      eElemNamesSql: Seq[String],
      ecElemNames: Seq[String],
      ecElemNamesSql: Seq[String],
      idIndex: Int
  )(using
      eCodec: DbCodec[E],
      ecCodec: DbCodec[EC],
      eClassTag: ClassTag[E],
      ecClassTag: ClassTag[EC],
      idClassTag: ClassTag[ID]
  ): RepoDefaults[EC, E, ID]
