package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.util.{Failure, Success, Using}

class Sql(val query: String, val params: Vector[Any]):

  def run[E](using con: DbCon, dbReader: DbReader[E]): Vector[E] =
    Using.Manager(use =>
      val ps = use(
        con.connection.prepareStatement(query)
      )
      setValues(ps, params)
      val rs = use(ps.executeQuery())
      dbReader.build(rs)
    ) match
      case Failure(t)   => throw SqlException(t, this)
      case Success(res) => res

  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runUpdate(using con: DbCon): Int =
    Using.Manager(use =>
      val ps = use(con.connection.prepareStatement(query))
      setValues(ps, params)
      ps.executeUpdate()
    ) match
      case Failure(t)   => throw SqlException(t, this)
      case Success(res) => res

  // todo stored procedures

  override def toString: String =
    query + "\n" + params
