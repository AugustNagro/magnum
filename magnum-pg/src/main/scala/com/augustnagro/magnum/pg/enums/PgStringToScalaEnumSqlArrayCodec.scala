package com.augustnagro.magnum.pg.enums

import com.augustnagro.magnum.DerivingUtil
import com.augustnagro.magnum.pg.SqlArrayCodec

import java.sql.JDBCType
import scala.deriving.Mirror
import scala.compiletime.{
  constValue,
  constValueTuple,
  erasedValue,
  error,
  summonFrom,
  summonInline
}
import scala.quoted.*
import scala.reflect.ClassTag

/** Supports mapping between Postgres Strings (Varchar, text, etc) and Scala
  * Enums in multi-dimensional arrays.
  */
inline given PgStringToScalaEnumSqlArrayCodec[
    A <: scala.reflect.Enum: Mirror.SumOf: ClassTag
]: SqlArrayCodec[A] =
  ${ pgStringToScalaEnumSqlArrayCodecImpl[A] }

private def pgStringToScalaEnumSqlArrayCodecImpl[A: Type](using
    Quotes
): Expr[SqlArrayCodec[A]] =
  import quotes.reflect.*
  val mirror = Expr.summon[Mirror.SumOf[A]].getOrElse {
    report.errorAndAbort(
      "Can only derive SqlArrayCodec for simple (non ADT) Enums"
    )
  }
  mirror match
    case '{
          $ms: Mirror.SumOf[A] {
            type MirroredElemTypes = mets
            type MirroredElemLabels = mels
            type MirroredLabel = mel
          }
        } =>
      val nameMapExpr = DerivingUtil.buildSqlNameMapForEnum[A, mels, mets]
      val melExpr = Expr(Type.valueOfConstant[mel].get.toString)
      val classTagExpr = Expr.summon[ClassTag[A]].get
      '{
        new SqlArrayCodec[A]:
          val nameMap = $nameMapExpr
          val ct = $classTagExpr
          val jdbcTypeName: String = JDBCType.VARCHAR.getName
          def readArray(array: Object): Array[A] =
            array
              .asInstanceOf[Array[String]]
              .map(enumName =>
                nameMap.find((k, _) => k == enumName) match
                  case Some((_, v)) => v
                  case None =>
                    throw IllegalArgumentException(
                      enumName + " not convertible to " + $melExpr
                    )
              )(using ct)
          def toArrayObj(entity: A): Object =
            nameMap.find((_, v) => v == entity) match
              case Some((k, _)) => k
              case None =>
                throw IllegalArgumentException(
                  entity.toString + " not convertible to " + $melExpr
                )
      }
  end match
end pgStringToScalaEnumSqlArrayCodecImpl
