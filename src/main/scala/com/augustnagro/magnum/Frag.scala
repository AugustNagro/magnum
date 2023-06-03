package com.augustnagro.magnum

import java.lang.System.Logger.Level
import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.util.{Failure, Success, Using}

/** Sql fragment */
case class Frag(query: String, params: Vector[Any]):
  def query[E](using codec: DbCodec[E]): Query[E] = Query(this, codec)
  def update: Update = Update(this)