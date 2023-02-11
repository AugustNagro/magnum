package com.augustnagro.magnum

import java.sql.ResultSet
import scala.deriving.Mirror
import scala.compiletime.{
  constValue,
  constValueTuple,
  erasedValue,
  error,
  summonInline
}
import scala.compiletime.ops.any.==
import scala.compiletime.ops.boolean.&&
import scala.reflect.ClassTag
import scala.quoted.*

private[magnum] object Util:

  transparent inline def buildCols[T](
      dbTableName: String,
      nameMapper: SqlNameMapper
  ) = ${ buildColsImpl[T]('dbTableName, 'nameMapper) }

  def buildColsImpl[T: Type](
      dbTableName: Expr[String],
      nameMapper: Expr[SqlNameMapper]
  )(using
      Quotes
  ): Expr[Any] =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[T]].get match
      case '{
            $m: Mirror.ProductOf[T] {
              type MirroredElemLabels = mels
              type MirroredElemTypes = mets
            }
          } =>
        buildRefinement[mels, Cols](dbTableName, nameMapper)

  private def buildRefinement[Mels: Type, XX: Type](
      dbTableName: Expr[String],
      nameMapper: Expr[SqlNameMapper]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[EmptyTuple] =>
        val res = '{ Cols($dbTableName, $nameMapper).asInstanceOf[XX] }
        println(res.show)
        res
      case '[mel *: melTail] =>
        val label = Type.valueOfConstant[mel].get.toString
        val newRefinement =
          Refinement(TypeRepr.of[XX], label, TypeRepr.of[String])
        newRefinement.asType match
          case '[tpe] =>
            buildRefinement[melTail, tpe](dbTableName, nameMapper)

  inline def getFromRow[Met](rs: ResultSet, columnIndex: Int): Any =
    inline erasedValue[Met] match
      case _: String                => rs.getString(columnIndex)
      case _: Boolean               => rs.getBoolean(columnIndex)
      case _: Byte                  => rs.getByte(columnIndex)
      case _: Short                 => rs.getShort(columnIndex)
      case _: Int                   => rs.getInt(columnIndex)
      case _: Long                  => rs.getLong(columnIndex)
      case _: Float                 => rs.getFloat(columnIndex)
      case _: Double                => rs.getDouble(columnIndex)
      case _: Array[Byte]           => rs.getBytes(columnIndex)
      case _: java.sql.Date         => rs.getDate(columnIndex)
      case _: java.sql.Time         => rs.getTime(columnIndex)
      case _: java.sql.Timestamp    => rs.getTimestamp(columnIndex)
      case _: java.sql.Ref          => rs.getRef(columnIndex)
      case _: java.sql.Blob         => rs.getBlob(columnIndex)
      case _: java.sql.Clob         => rs.getClob(columnIndex)
      case _: java.net.URL          => rs.getURL(columnIndex)
      case _: java.sql.RowId        => rs.getRowId(columnIndex)
      case _: java.sql.NClob        => rs.getNClob(columnIndex)
      case _: java.sql.SQLXML       => rs.getSQLXML(columnIndex)
      case _: scala.math.BigDecimal => BigDecimal(rs.getBigDecimal(columnIndex))
      case _: java.math.BigDecimal  => rs.getBigDecimal(columnIndex)
      case _: scala.math.BigInt =>
        scala.math.BigInt(
          rs.getObject(columnIndex, classOf[java.math.BigInteger])
        )
      case _: Option[t] => Option(getFromRow[t](rs, columnIndex))
      case _ =>
        rs.getObject(columnIndex, summonInline[ClassTag[Met]].runtimeClass)
