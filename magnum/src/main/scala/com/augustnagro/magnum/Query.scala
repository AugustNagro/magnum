package com.augustnagro.magnum

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Using.Manager
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try, Using}

class Query[E] private[magnum] (val frag: Frag, reader: DbCodec[E]):

  def run()(using con: DbCon): Vector[E] =
    handleQuery(frag.sqlString, frag.params):
      Using.Manager: use =>
        val ps = use(con.connection.prepareStatement(frag.sqlString))
        frag.writer.write(ps, 1)
        timed:
          val rs = use(ps.executeQuery())
          reader.read(rs)

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
          val rs = use(ps.executeQuery())
          ResultSetIterator(rs, frag, reader, con.sqlLogger)

end Query
