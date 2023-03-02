package com.augustnagro.magnum

import java.sql.ResultSet
import java.time.{LocalDate, LocalTime, LocalDateTime}
import scala.annotation.implicitNotFound
import scala.deriving.Mirror
import scala.compiletime.{
  constValue,
  constValueTuple,
  erasedValue,
  error,
  summonInline
}

/** Typeclass defining database entities. Has methods to build E from a JDBC
  * ResultSet
  */
trait DbReader[E]:

  /** Build an E from the ResultSet. Make sure the ResultSet is in a valid state
    * (ie, ResultSet::next has been called).
    */
  def buildSingle(rs: ResultSet): E

  /** Build every row in the ResultSet into a sequence of E. The ResultSet
    * should be in its initial position before calling (ie, ResultSet::next not
    * called).
    */
  def build(rs: ResultSet): Vector[E] =
    val res = Vector.newBuilder[E]
    while rs.next() do res += buildSingle(rs)
    res.result()

object DbReader:

  given DbReader[String] = rs => rs.getString(1)
  given DbReader[Boolean] = rs => rs.getBoolean(1)
  given DbReader[Byte] = rs => rs.getByte(1)
  given DbReader[Short] = rs => rs.getShort(1)
  given DbReader[Int] = rs => rs.getInt(1)
  given DbReader[Long] = rs => rs.getLong(1)
  given DbReader[Float] = rs => rs.getFloat(1)
  given DbReader[Double] = rs => rs.getDouble(1)
  given DbReader[Array[Byte]] = rs => rs.getBytes(1)
  given DbReader[java.sql.Date] = rs => rs.getDate(1)
  given DbReader[LocalDate] = rs => rs.getDate(1).toLocalDate
  given DbReader[java.sql.Time] = rs => rs.getTime(1)
  given DbReader[LocalTime] = rs => rs.getTime(1).toLocalTime
  given DbReader[java.sql.Timestamp] = rs => rs.getTimestamp(1)
  given DbReader[LocalDateTime] = rs => rs.getTimestamp(1).toLocalDateTime
  given DbReader[java.sql.Ref] = rs => rs.getRef(1)
  given DbReader[java.sql.Blob] = rs => rs.getBlob(1)
  given DbReader[java.sql.Clob] = rs => rs.getClob(1)
  given DbReader[java.net.URL] = rs => rs.getURL(1)
  given DbReader[java.sql.RowId] = rs => rs.getRowId(1)
  given DbReader[java.sql.NClob] = rs => rs.getNClob(1)
  given DbReader[java.sql.SQLXML] = rs => rs.getSQLXML(1)
  given DbReader[scala.math.BigDecimal] = rs => BigDecimal(rs.getBigDecimal(1))
  given DbReader[scala.math.BigInt] = rs =>
    BigInt(rs.getObject(1, classOf[java.math.BigInteger]))
  given DbReader[A: DbReader]: DbReader[Option[A]] =
    rs => Option(summon[DbReader[A]].buildSingle(rs))
  given DbReader[Unit] = rs => ()

  inline given derived[E](using m: Mirror.ProductOf[E]): DbReader[E] =
    type Mets = m.MirroredElemTypes
    inline val arity = constValue[Tuple.Size[Mets]]
    new DbReader[E]:
      def buildSingle(rs: ResultSet): E =
        buildSingleDefault[E, m.MirroredElemTypes](
          rs,
          m,
          Array.ofDim[Any](arity)
        )

private inline def buildSingleDefault[E, Mets](
    rs: ResultSet,
    m: Mirror.ProductOf[E],
    res: Array[Any],
    i: Int = 0
): E =
  inline erasedValue[Mets] match
    case _: EmptyTuple => m.fromProduct(ArrayProduct(res))
    case _: (met *: metTail) =>
      res(i) = getFromRow[met](rs, i + 1)
      buildSingleDefault[E, metTail](rs, m, res, i + 1)
