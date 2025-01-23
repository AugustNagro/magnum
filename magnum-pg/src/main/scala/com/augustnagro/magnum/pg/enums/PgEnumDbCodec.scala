package com.augustnagro.magnum.pg.enums

import com.augustnagro.magnum.{DbCodec, DerivingUtil}

import java.sql.{JDBCType, PreparedStatement, ResultSet}
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

inline given PgEnumDbCodec[A <: scala.reflect.Enum: Mirror.SumOf]: DbCodec[A] =
  val codec = DbCodec.derived[A]
  pgEnumDbCodecImpl[A]

private def pgEnumDbCodecImpl[A: Type](baseCodec: Expr[DbCodec[A]])(using
    Quotes
): Expr[DbCodec[A]] =
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
      val melExpr = Expr(Type.valueOfConstant[mel].get.toString)
      val sqlTypeNameExpr: Expr[String] =
        DerivingUtil
          .sqlTableNameAnnot[A]
          .map(sqlNameExpr => '{ $sqlNameExpr.name })
          .orElse(
            DerivingUtil
              .tableAnnot[A]
              .map(tableExpr =>
                '{ $tableExpr.nameMapper.toTableName($melExpr) }
              )
          )
          .getOrElse(melExpr)
      '{
        val c = $baseCodec
        new DbCodec[A]:
          def queryRepr: String = "?::" + ${ sqlTypeNameExpr }
          def cols: IArray[Int] = c.cols
          def readSingle(resultSet: ResultSet, pos: Int): A =
            c.readSingle(resultSet, pos)
          def readSingleOption(resultSet: ResultSet, pos: Int): Option[A] =
            c.readSingleOption(resultSet, pos)
          def writeSingle(entity: A, ps: PreparedStatement, pos: Int): Unit =
            c.writeSingle(entity, ps, pos)
      }
  end match
end pgEnumDbCodecImpl
