package com.augustnagro.magnum

import java.sql.{ResultSet, Statement}

class SqlBuilder(query: String, params: Vector[Any]):

  def runPreparedRS()(using con: DbCon): ResultSet =
    val ps = con.manager(
      con.connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
    )
    // todo set values
    con.manager(ps.executeQuery())

  def runPrepared[E](using con: DbCon, dbe: DbEntity[E]): Vector[E] =
    val ps = con.manager(
      con.connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
    )
    // todo set values
    val rs = con.manager(ps.executeQuery())
    dbe.build(rs)

  def run[E](using con: DbCon, dbe: DbEntity[E]): Vector[E] =
    val s = con.manager(con.connection.createStatement())
    // todo set values
    val rs = con.manager(s.executeQuery(query))
    dbe.build(rs)

  def runPreparedBatch() = ???

  def runBatch() = ???

  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runPreparedUpdate()(using con: DbCon): Int = ???

  def runUpdate() = ???

  // todo stored procedures
