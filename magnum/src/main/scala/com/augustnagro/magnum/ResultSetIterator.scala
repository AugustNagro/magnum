package com.augustnagro.magnum

import java.sql.ResultSet

private class ResultSetIterator[E](
    rs: ResultSet,
    frag: Frag,
    reader: DbCodec[E]
) extends Iterator[E] {

  private var rsHasNext: Boolean =
    try rs.next()
    catch case t => throw SqlException(frag, t)

  override def hasNext: Boolean = rsHasNext

  override def next(): E =
    if !rsHasNext then throw IllegalStateException("ResultSet is empty")
    try
      val e = reader.readSingle(rs)
      rsHasNext = rs.next()
      e
    catch case t => throw SqlException(frag, t)

}
