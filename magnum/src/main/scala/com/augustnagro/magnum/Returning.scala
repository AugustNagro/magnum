package com.augustnagro.magnum

import scala.util.{Failure, Success, Try, Using}
import Using.Manager
import java.sql.Statement
import java.sql.ResultSet

class Returning[E] private[magnum] (
    val frag: Frag,
    reader: DbCodec[E],
    keyColumns: Iterable[String]
):
  private val keyColumsArr = keyColumns.toArray

  def run()(using con: DbCon): Vector[E] =
    withResultSet(reader.read)

  /** Streaming [[Iterator]]. Set [[fetchSize]] to give the JDBC driver a hint
    * as to how many rows to fetch per request
    */
  def iterator(
      fetchSize: Int = 0
  )(using con: DbCon, use: Manager): Iterator[E] =
    withResultSet(ResultSetIterator(_, frag, reader, con.sqlLogger))

  private def withResultSet[A](f: ResultSet => A)(using con: DbCon): A =
    handleQuery(frag.sqlString, frag.params):
      Manager: use =>
        if keyColumsArr.isEmpty then
          val ps = use(con.connection.prepareStatement(frag.sqlString))
          frag.writer.write(ps, 1)
          timed:
            val hasResults = ps.execute()
            if hasResults then
              val rs = use(ps.getResultSet)
              f(rs)
            else
              throw UnsupportedOperationException(
                "No results for RETURNING clause"
              )
        else
          val ps = use(
            con.connection.prepareStatement(frag.sqlString, keyColumsArr)
          )
          frag.writer.write(ps, 1)
          timed:
            ps.execute()
            val rs = use(ps.getGeneratedKeys)
            f(rs)

end Returning
