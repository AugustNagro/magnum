package com.augustnagro.magnum

import Sql.*

import java.lang.System.Logger.Level
import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.util.{Failure, Success, Using}

case class Sql(query: String, params: Vector[Any]):

  def run[E](using con: DbCon, dbReader: DbReader[E]): Vector[E] =
    Using.Manager(use =>
      logSql(this)
      val ps = use(con.connection.prepareStatement(query))
      setValues(ps, params)
      val rs = use(ps.executeQuery())
      dbReader.build(rs)
    ) match
      case Failure(t) => throw SqlException(t, this)
      case Success(res) => res

  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runUpdate(using con: DbCon): Int =
    Using.Manager(use =>
      logSql(this)
      val ps = use(con.connection.prepareStatement(query))
      setValues(ps, params)
      ps.executeUpdate()
    ) match
      case Failure(t)   => throw SqlException(t, this)
      case Success(res) => res

  override def toString: String =
    query + "\n" + params

object Sql:
  private val Log = System.getLogger(classOf[Sql].getName)
