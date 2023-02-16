package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet, Statement}

class Sql(query: String, params: Vector[Any]):

  def runRS()(using con: DbCon): ResultSet =
    val ps = con.manager(
      con.connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
    )
    Util.setValues(ps, params)
    con.manager(ps.executeQuery())

  def run[E](using con: DbCon, dbe: DbReader[E]): Vector[E] =
    val ps = con.manager(
      con.connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
    )
    Util.setValues(ps, params)
    val rs = con.manager(ps.executeQuery())
    dbe.build(rs)

  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runUpdate()(using con: DbCon): Int = ???

  // todo stored procedures

  override def toString: String =
    query + "\n" + params
