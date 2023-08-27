package com.augustnagro.magnum

import scala.util.{Failure, Success, Using}, Using.Manager
import java.sql.Statement

case class Returning[E](frag: Frag, reader: DbCodec[E]):
  def run()(using con: DbCon): Vector[E] =
    logSql(frag)
    Manager(use =>
      val ps = use(
        con.connection
          .prepareStatement(frag.sqlString)
      )
      frag.writer.write(ps, 1)
      val hasResults = ps.execute()
      if hasResults then
        val rs = use(ps.getResultSet())
        reader.read(rs)
      else
        throw UnsupportedOperationException("No results for RETURNING clause")
    ) match
      case Success(res) => res
      case Failure(t)   => throw SqlException(frag, t)
