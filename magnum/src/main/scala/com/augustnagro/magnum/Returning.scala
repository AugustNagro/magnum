package com.augustnagro.magnum

import scala.util.{Failure, Success, Try, Using}
import Using.Manager
import java.sql.Statement

case class Returning[E](frag: Frag, reader: DbCodec[E]):
  def run()(using con: DbCon): Vector[E] =
    handleQuery(frag.sqlString, frag.params):
      Manager: use =>
        val ps = use(con.connection.prepareStatement(frag.sqlString))
        frag.writer.write(ps, 1)
        timed:
          val hasResults = ps.execute()
          if hasResults then
            val rs = use(ps.getResultSet())
            reader.read(rs)
          else
            throw UnsupportedOperationException(
              "No results for RETURNING clause"
            )

  /** Streaming [[Iterator]]. Set [[fetchSize]] to give the JDBC driver a hint
    * as to how many rows to fetch per request
    */
  def iterator(
      fetchSize: Int = 0
  )(using con: DbCon, use: Manager): Iterator[E] =
    handleQuery(frag.sqlString, frag.params):
      Try:
        val ps = use(con.connection.prepareStatement(frag.sqlString))
        ps.setFetchSize(fetchSize)
        frag.writer.write(ps, 1)
        timed:
          val hasResults = ps.execute()
          if hasResults then
            val rs = use(ps.getResultSet())
            ResultSetIterator(rs, frag, reader, con.sqlLogger)
          else
            throw UnsupportedOperationException(
              "No results for RETURNING clause"
            )

end Returning
