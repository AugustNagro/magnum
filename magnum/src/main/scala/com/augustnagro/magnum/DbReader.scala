package com.augustnagro.magnum

import java.sql.ResultSet
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

  given DbReader[Int] = rs => rs.getInt(1)
  
  given DbReader[Long] = rs => rs.getLong(1)
  
  // todo more

  inline given derived[E](using m: Mirror.ProductOf[E]): DbReader[E] =
    type Mets = m.MirroredElemTypes
    inline val arity = constValue[Tuple.Size[Mets]]
    (rs: ResultSet) =>
      buildSingleDefault[E, m.MirroredElemTypes](rs, m, Array.ofDim[Any](arity))

end DbReader

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
