package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet, Statement}

class Sql(val query: String, val params: Vector[Any]):

  def runRS()(using con: DbCon): ResultSet =
    try
      val ps = con.manager(
        con.connection.prepareStatement(query)
      )
      setValues(ps, params)
      con.manager(ps.executeQuery())
    catch
      case e: Exception =>
        throw SqlException(e, this)

  def run[E](using con: DbCon, dbe: DbReader[E]): Vector[E] =
    try
      val ps = con.manager(
        con.connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
      )
      setValues(ps, params)
      val rs = con.manager(ps.executeQuery())
      dbe.build(rs)
    catch
      case e: Exception =>
        throw SqlException(e, this)

  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runUpdate(using con: DbCon): Int = ???

  // todo stored procedures

  override def toString: String =
    query + "\n" + params
