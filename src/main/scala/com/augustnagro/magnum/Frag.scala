package com.augustnagro.magnum

import java.lang.System.Logger.Level
import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Using}

/** Sql fragment */
case class Frag(
    sqlString: String,
    params: Seq[Any] = Seq.empty,
    writer: FragWriter = Frag.emptyWriter
):
  def query[E](using reader: DbCodec[E]): Query[E] = Query(this, reader)
  def update: Update = Update(this)
  def returning[E](using reader: DbCodec[E]): Returning[E] = Returning(this)

object Frag:
  private val emptyWriter: FragWriter = (_, _) => 0
