package com.augustnagro.magnum

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

/** Not useful for typical user code; provided to help implement custom DbCodecs
  * and associated typeclasses
  */
object DerivingUtil:
  /** For a Simple (non-ADT) enum type E, constructs a sequence of mappings from
    * sql string representation to enum value. For example,
    *
    * {{{
    *   @Table(PostgresDbType, SqlNameMapper.CamelToUpperSnakeCase)
    *   enum Color { case Red, @SqlName("greeeeeen") Green, Blue }
    * }}}
    *
    * Results in
    *
    * {{{
    *   Seq("Red" -> Color.Red, "greeeeeen" -> Color.Green, "Blue" -> Color.Blue)
    * }}}
    *
    * Will produce a compile error if the enum is not simple (non-adt).
    *
    * @tparam E
    *   the enum type, like Color
    * @tparam Mels
    *   enum Mirror's MirroredElemLabels
    * @tparam Mets
    *   enum Mirror's MirroredElemTypes
    */
  def buildSqlNameMapForEnum[
      E: Type,
      Mels: Type,
      Mets: Type
  ](using q: Quotes): Expr[Seq[(String, E)]] =
    import q.reflect.*
    val tableAnnot = TypeRepr.of[Table].typeSymbol
    val defaultNameMapper: Expr[SqlNameMapper] =
      TypeRepr
        .of[E]
        .typeSymbol
        .getAnnotation(tableAnnot) match
        case Some(term) =>
          val tableExpr = term.asExprOf[Table]
          '{ $tableExpr.nameMapper }
        case None =>
          '{ SqlNameMapper.SameCase }

    val sumValueExprs: Vector[Expr[E]] = sumValues[E, Mets]()
    val scalaNames = getScalaNames[Mels]()

    val sqlNameAnnot = TypeRepr.of[SqlName].typeSymbol
    val enumCaseSymbols = TypeRepr.of[E].typeSymbol.children

    val sqlNameExprs: Vector[Expr[(String, E)]] = scalaNames
      .zip(sumValueExprs)
      .map((scalaName, sumExpr) =>
        val nameAnnot = enumCaseSymbols
          .find(sym => sym.name == scalaName && sym.hasAnnotation(sqlNameAnnot))
          .flatMap(sym => sym.getAnnotation(sqlNameAnnot))
        nameAnnot match
          case Some(term) =>
            val sqlNameExpr: Expr[SqlName] = term.asExprOf[SqlName]
            '{ ($sqlNameExpr.name.toString, $sumExpr) }
          case None =>
            val scalaNameExpr = Expr(scalaName)
            '{ ($defaultNameMapper.toColumnName($scalaNameExpr), $sumExpr) }
      )
    Expr.ofSeq(sqlNameExprs)
  end buildSqlNameMapForEnum

  private def getScalaNames[Mels: Type](res: Vector[String] = Vector.empty)(
      using Quotes
  ): Vector[String] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val melString = Type.valueOfConstant[mel].get.toString
        getScalaNames[melTail](res :+ melString)
      case '[EmptyTuple] => res

  private def sumValues[E: Type, Mets: Type](
      res: Vector[Expr[E]] = Vector.empty
  )(using Quotes): Vector[Expr[E]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val expr = Expr.summon[Mirror.ProductOf[met]] match
          case Some(m) if isSingleton[met] =>
            '{ $m.fromProduct(EmptyTuple).asInstanceOf[E] }
          case _ =>
            report.errorAndAbort("Can only derive simple (non-adt) enums")
        sumValues[E, metTail](res :+ expr)
      case '[EmptyTuple] => res

  private def isSingleton[T: Type](using Quotes): Boolean =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[T]] match
      case Some('{
            $mp: Mirror.ProductOf[T] {
              type MirroredElemTypes = mets
            }
          }) =>
        tupleArity[mets]() == 0
      case _ => false

  private def tupleArity[T: Type](res: Int = 0)(using Quotes): Int =
    import quotes.reflect.*
    Type.of[T] match
      case '[x *: xs]    => tupleArity[xs](res + 1)
      case '[EmptyTuple] => res

  /** Finds the first SqlName annotation on type T */
  def sqlTableNameAnnot[T: Type](using Quotes): Option[Expr[SqlName]] =
    import quotes.reflect._
    val annot = TypeRepr.of[SqlName]
    TypeRepr
      .of[T]
      .typeSymbol
      .annotations
      .find(_.tpe =:= annot)
      .map(term => term.asExprOf[SqlName])

  /** Finds the first Table annotation on type T */
  def tableAnnot[T: Type](using Quotes): Option[Expr[Table]] =
    import quotes.reflect.*
    val annot = TypeRepr.of[Table]
    TypeRepr
      .of[T]
      .typeSymbol
      .annotations
      .find(_.tpe =:= annot)
      .map(term => term.asExprOf[Table])

end DerivingUtil
