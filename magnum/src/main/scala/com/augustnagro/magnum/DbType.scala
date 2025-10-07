package com.augustnagro.magnum

import DbType.IdMeta

import scala.reflect.ClassTag
import scala.deriving.Mirror
import scala.quoted.Expr

/** Factory for Repo default methods */
trait DbType:
  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      eElemNames: Seq[String],
      eElemNamesSql: Seq[String],
      eElemCodecs: Seq[DbCodec[?]],
      ecElemNames: Seq[String],
      ecElemNamesSql: Seq[String],
      ids: Seq[IdMeta]
  )(using
      eCodec: DbCodec[E],
      ecCodec: DbCodec[EC],
      eClassTag: ClassTag[E],
      ecClassTag: ClassTag[EC],
  ): RepoDefaults[EC, E, ID]

object DbType:
  case class IdMeta(index: Int, codec: DbCodec[?], classTag: ClassTag[?])
