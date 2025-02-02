package com.augustnagro.magnum.pg.enums

import com.augustnagro.magnum.{DbCodec, DerivingUtil}

import java.sql.{JDBCType, PreparedStatement, ResultSet, Types}
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
  ${ pgEnumDbCodecImpl[A] }

private def pgEnumDbCodecImpl[A: Type](using Quotes): Expr[DbCodec[A]] =
  import quotes.reflect.*
  val mirror = Expr.summon[Mirror.SumOf[A]].getOrElse {
    report.errorAndAbort(
      "Can only derive PgEnumDbCodec for simple (non ADT) Enums"
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
        new DbCodec[A] {
          val nameMap: Seq[(String, A)] = $nameMapExpr
          val cols: IArray[Int] = IArray(Types.VARCHAR)
          def readSingle(rs: ResultSet, pos: Int): A =
            val str = rs.getString(pos)
            nameMap.find((name, _) => name == str) match
              case Some((_, v)) => v
              case None =>
                throw IllegalArgumentException(
                  str + " not convertible to " + $melExpr
                )
          def readSingleOption(rs: ResultSet, pos: Int): Option[A] =
            Option(rs.getString(pos)).map(str =>
              nameMap.find((name, _) => name == str) match
                case Some((_, v)) => v
                case None =>
                  throw IllegalArgumentException(
                    str + " not convertible to " + $melExpr
                  )
            )
          def writeSingle(entity: A, ps: PreparedStatement, pos: Int): Unit =
            nameMap.find((_, v) => v == entity) match
              case Some((k, _)) => ps.setString(pos, k)
              case None =>
                throw IllegalArgumentException(
                  entity.toString + " not convertible to " + $melExpr
                )
          def queryRepr: String = "?::" + ${ sqlTypeNameExpr }
        }
      }
  end match
end pgEnumDbCodecImpl
