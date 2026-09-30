package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet}
import scala.compiletime.*
import scala.collection.immutable.ArraySeq
import scala.deriving.*
import scala.quoted.*
import scala.reflect.ClassTag
import scala.util.boundary

trait RepoDefaults[EC, E, ID]:
  def count(using DbCon): Long
  def existsById(id: ID)(using DbCon): Boolean
  def findAll(using DbCon): Vector[E]
  def findAll(spec: Spec[E])(using DbCon): Vector[E]
  def findById(id: ID)(using DbCon): Option[E]
  def findAllById(ids: Iterable[ID])(using DbCon): Vector[E]
  def delete(entity: E)(using DbCon): Unit
  def deleteById(id: ID)(using DbCon): Unit
  def truncate()(using DbCon): Unit
  def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult
  def deleteAllById(ids: Iterable[ID])(using DbCon): BatchUpdateResult
  def insert(entityCreator: EC)(using DbCon): Unit
  def insertAll(entityCreators: Iterable[EC])(using DbCon): Unit
  def insertReturning(entityCreator: EC)(using DbCon): E
  def insertAllReturning(entityCreators: Iterable[EC])(using DbCon): Vector[E]
  def update(entity: E)(using DbCon): Unit
  def updateAll(entities: Iterable[E])(using DbCon): BatchUpdateResult

object RepoDefaults:

  inline given genImmutableRepo[E: DbCodec: Mirror.Of, ID]
      : RepoDefaults[E, E, ID] =
    genRepo[E, E, ID]

  inline given genRepo[
      EC: DbCodec: Mirror.Of,
      E: DbCodec: Mirror.Of,
      ID
  ]: RepoDefaults[EC, E, ID] = ${ genImpl[EC, E, ID] }

  @scala.annotation.publicInBinary
  private[magnum] def genImpl[EC: Type, E: Type, ID: Type](using
      Quotes
  ): Expr[RepoDefaults[EC, E, ID]] =
    import quotes.reflect.*
    val exprs = tableExprs[EC, E, ID]
    val eElemCodecs = getEElemCodecs[E]
    val eCodec = Expr.summon[DbCodec[E]].get
    val ecCodec = Expr.summon[DbCodec[EC]].get
    val eClassTag = Expr.summon[ClassTag[E]].get
    val ecClassTag = Expr.summon[ClassTag[EC]].get
    val idClassTag =
      if TypeRepr.of[ID] =:= TypeRepr.of[Null] then
        '{ ClassTag.Any.asInstanceOf[ClassTag[ID]] }
      else Expr.summon[ClassTag[ID]].get

    // function that builds ID from entity's ID fields
    val idFromProductExpr: Expr[Seq[Any] => ID] =
      if TypeRepr.of[ID] =:= TypeRepr.of[Null] then
        '{ (fields: Seq[Any]) => null.asInstanceOf[ID] }
      else
        Expr.summon[Mirror.ProductOf[ID]] match
          case Some('{ $m: Mirror.ProductOf[ID] }) =>
            '{ (fields: Seq[Any]) =>
              $m.fromProduct(Tuple.fromArray(fields.toArray)).asInstanceOf[ID]
            }
          case _ =>
            '{ (fields: Seq[Any]) =>
              fields.headOption
                .getOrElse(
                  throw new IllegalArgumentException(
                    s"Cannot construct ID from ${fields.size} fields"
                  )
                )
                .asInstanceOf[ID]
            }
    '{
      val elemCodecs = $eElemCodecs
      val idIndices = ${ exprs.idIndices }
      val idFromProduct = ${ idFromProductExpr }
      val idCodec = RepoDefaults.idCodec[ID](
        idIndices,
        elemCodecs,
        idFromProduct
      )
      ${ exprs.tableAnnot }.dbType.buildRepoDefaults[EC, E, ID](
        ${ exprs.tableNameSql },
        ${ Expr(exprs.eElemNames) },
        ${ Expr.ofSeq(exprs.eElemNamesSql) },
        elemCodecs,
        ${ Expr(exprs.ecElemNames) },
        ${ Expr.ofSeq(exprs.ecElemNamesSql) },
        idIndices,
        idFromProduct
      )(using
        $eCodec,
        $ecCodec,
        idCodec,
        $eClassTag,
        $ecClassTag,
        $idClassTag
      )
    }
  end genImpl

  @scala.annotation.publicInBinary
  private[RepoDefaults] def idCodec[ID](
      idIndices: Seq[Int],
      eElemCodecs: Seq[DbCodec[?]],
      idFromProduct: Seq[Any] => ID
  ): DbCodec[ID] =
    val codecs =
      IArray.from(idIndices.map(eElemCodecs(_).asInstanceOf[DbCodec[Any]]))
    codecs match
      case IArray()      => DbCodec.AnyCodec.asInstanceOf[DbCodec[ID]]
      case IArray(codec) => codec.asInstanceOf[DbCodec[ID]]
      case _             =>
        new DbCodec[ID]:
          val cols: IArray[Int] = codecs.flatMap(_.cols)
          val queryRepr: String = codecs.map(_.queryRepr).mkString(", ")

          def readSingle(rs: ResultSet, pos: Int): ID =
            val res = Array.ofDim[Any](codecs.length)
            var col = pos
            var i = 0
            while i < codecs.length do
              val codec = codecs(i)
              res(i) = codec.readSingle(rs, col)
              col += codec.cols.length
              i += 1
            idFromProduct(IArray.unsafeFromArray(res))

          def readSingleOption(rs: ResultSet, pos: Int): Option[ID] =
            boundary:
              val res = Array.ofDim[Any](codecs.length)
              var col = pos
              var i = 0
              while i < codecs.length do
                val codec = codecs(i)
                codec.readSingleOption(rs, col) match
                  case Some(value) => res(i) = value
                  case None        => boundary.break(None)
                col += codec.cols.length
                i += 1
              Some(idFromProduct(IArray.unsafeFromArray(res)))

          def writeSingle(id: ID, ps: PreparedStatement, pos: Int): Unit =
            val product = id.asInstanceOf[Product]
            var col = pos
            var i = 0
            while i < codecs.length do
              val codec = codecs(i)
              codec.writeSingle(product.productElement(i), ps, col)
              col += codec.cols.length
              i += 1
    end match
  end idCodec

  private def getEElemCodecs[E: Type](using Quotes): Expr[Seq[DbCodec[?]]] =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $m: Mirror.ProductOf[E] {
              type MirroredElemTypes = mets
            }
          }) =>
        getProductCodecs[E, mets]()
      case _ =>
        val sumCodec = Expr.summon[DbCodec[E]].get
        '{ Seq($sumCodec) }

  private def getProductCodecs[E: Type, Mets: Type](
      res: Vector[Expr[DbCodec[?]]] = Vector.empty
  )(using Quotes): Expr[Seq[DbCodec[?]]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val codec = DerivingUtil
          .fieldCodec[E, met](res.size)
          .orElse(
            Expr
              .summon[DbCodec[met]]
              .orElse(
                TypeRepr.of[met].widen.asType match
                  case '[tpe] =>
                    Expr
                      .summon[DbCodec[tpe]]
                      .map(codec => '{ $codec.asInstanceOf[DbCodec[met]] })
              )
          )
          .getOrElse(
            report.errorAndAbort(
              s"Could not find given DbCodec for ${TypeRepr.of[met].show}."
            )
          )
        getProductCodecs[E, metTail](res :+ codec)
      case '[EmptyTuple] => Expr.ofSeq(res)
    end match
  end getProductCodecs

end RepoDefaults
