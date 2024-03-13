package com.augustnagro.magnum

import scala.util.Using.Manager
import scala.util.{Failure, Success, Using}

case class Query[E](frag: Frag, reader: DbCodec[E]):

  def run()(using con: DbCon): Vector[E] =
    logSql(frag)
    Using.Manager(use =>
      val ps = use(con.connection.prepareStatement(frag.sqlString))
      frag.writer.write(ps, 1)
      val rs = use(ps.executeQuery())
      reader.read(rs)
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
      val rs = use(ps.executeQuery())
      ResultSetIterator(rs, frag, reader)
    catch case t => throw SqlException(frag, t)

end Query
