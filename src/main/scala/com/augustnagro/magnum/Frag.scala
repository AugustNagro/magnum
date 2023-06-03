package com.augustnagro.magnum

import java.lang.System.Logger.Level
import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.util.{Failure, Success, Using}

/** Sql fragment */
case class Frag(query: String, params: Vector[Any]):

  def run[E](using con: DbCon, dbReader: DbCodec[E]): Vector[E] =
    logSql(this)
    Using.Manager(use =>
      val ps = use(con.connection.prepareStatement(query))
      setValues(ps, params)
      val rs = use(ps.executeQuery())
      dbReader.read(rs)
    ) match
      case Success(res) => res
      case Failure(t)   => throw SqlException(query, params, t)

  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runUpdate(using con: DbCon): Int =
    logSql(this)
    Using(con.connection.prepareStatement(query))(ps =>
      setValues(ps, params)
      ps.executeUpdate()
    ) match
      case Success(res) => res
      case Failure(t)   => throw SqlException(query, params, t)
