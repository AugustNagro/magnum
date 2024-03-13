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

  /** Streaming [[Iterator]]. Set [[fetchSize]] to give the JDBC driver a hint
    * as to how many rows to fetch per request
    */
  def iterator(
      fetchSize: Int = 0
  )(using con: DbCon, use: Manager): Iterator[E] =
    logSql(frag)
    try
      val ps = use(con.connection.prepareStatement(frag.sqlString))
      ps.setFetchSize(fetchSize)
      frag.writer.write(ps, 1)
      val hasResults = ps.execute()
      if hasResults then
        val rs = use(ps.getResultSet())
        ResultSetIterator(rs, frag, reader)
      else
        throw UnsupportedOperationException("No results for RETURNING clause")
    catch case t => throw SqlException(frag, t)

end Returning
