package com.augustnagro.magnum.streaming

import com.augustnagro.magnum.builders.Frag
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.errors.SqlException
import com.augustnagro.magnum.logging.{SqlExceptionEvent, SqlLogger}

import java.sql.ResultSet
import scala.util.control.NonFatal

private[magnum] class ResultSetIterator[E](
    rs: ResultSet,
    frag: Frag,
    reader: DbCodec[E],
    sqlLogger: SqlLogger
) extends Iterator[E] {

  private var rsHasNext: Boolean =
    try rs.next()
    catch
      case NonFatal(t) =>
        throw SqlException(
          sqlLogger.exceptionMsg(
            SqlExceptionEvent(frag.sqlString, frag.params, t)
          ),
          t
        )

  override def hasNext: Boolean = rsHasNext

  override def next(): E =
    if !rsHasNext then throw IllegalStateException("ResultSet is empty")
    try
      val e = reader.readSingle(rs)
      rsHasNext = rs.next()
      e
    catch
      case NonFatal(t) =>
        throw SqlException(
          sqlLogger.exceptionMsg(
            SqlExceptionEvent(frag.sqlString, frag.params, t)
          ),
          t
        )

}
