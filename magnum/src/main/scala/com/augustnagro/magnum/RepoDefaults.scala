package com.augustnagro.magnum

import scala.compiletime.*
import scala.deriving.*
import scala.quoted.*
import scala.reflect.ClassTag

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

  private def genImpl[EC: Type, E: Type, ID: Type](using
      Quotes
  ): Expr[RepoDefaults[EC, E, ID]] =
    import quotes.reflect.*
    val exprs = tableExprs[EC, E, ID]
    val eElemCodecs = getEElemCodecs[E]
    val eCodec = Expr.summon[DbCodec[E]].get
    val ecCodec = Expr.summon[DbCodec[EC]].get
    val idCodec =
      if TypeRepr.of[ID] =:= TypeRepr.of[Null] then
        '{ DbCodec.AnyCodec.asInstanceOf[DbCodec[ID]] }
      else Expr.summon[DbCodec[ID]].get
    val eClassTag = Expr.summon[ClassTag[E]].get
    val ecClassTag = Expr.summon[ClassTag[EC]].get
    val idClassTag =
      if TypeRepr.of[ID] =:= TypeRepr.of[Null] then
        '{ ClassTag.Any.asInstanceOf[ClassTag[ID]] }
      else Expr.summon[ClassTag[ID]].get
    '{
      ${ exprs.tableAnnot }.dbType.buildRepoDefaults[EC, E, ID](
        ${ exprs.tableNameSql },
        ${ Expr(exprs.eElemNames) },
        ${ Expr.ofSeq(exprs.eElemNamesSql) },
        $eElemCodecs,
        ${ Expr(exprs.ecElemNames) },
        ${ Expr.ofSeq(exprs.ecElemNamesSql) },
        ${ exprs.idIndex }
      )(using
        $eCodec,
        $ecCodec,
        $idCodec,
        $eClassTag,
        $ecClassTag,
        $idClassTag
      )
    }
  end genImpl

  private def getEElemCodecs[E: Type](using Quotes): Expr[Seq[DbCodec[?]]] =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $m: Mirror.ProductOf[E] {
              type MirroredElemTypes = mets
            }
          }) =>
        getProductCodecs[mets]()
      case _ =>
        val sumCodec = Expr.summon[DbCodec[E]].get
        '{ Seq($sumCodec) }

  private def getProductCodecs[Mets: Type](
      res: Vector[Expr[DbCodec[?]]] = Vector.empty
  )(using Quotes): Expr[Seq[DbCodec[?]]] =
    Type.of[Mets] match
      case '[met *: metTail] =>
        Expr.summon[DbCodec[met]] match
          case Some(codec) => getProductCodecs[metTail](res :+ codec)
          case None => getProductCodecs[metTail](res :+ '{ DbCodec.AnyCodec })
      case '[EmptyTuple] => Expr.ofSeq(res)

end RepoDefaults
