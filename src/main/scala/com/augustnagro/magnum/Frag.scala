package com.augustnagro.magnum

import java.lang.System.Logger.Level
import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.util.{Failure, Success, Using}

/** Sql fragment */
case class Frag(
    query: String,
    params: Seq[Any],
    writer: (ps: PreparedStatement, pos: Int) => Unit
):
  def query[E](using reader: DbCodec[E]): Query[E] = Query(this, reader)
  def update: Update = Update(this)
