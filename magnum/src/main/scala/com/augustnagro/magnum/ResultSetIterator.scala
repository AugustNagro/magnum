package com.augustnagro.magnum

import java.sql.ResultSet
import scala.util.control.NonFatal

private class ResultSetIterator[E](
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
