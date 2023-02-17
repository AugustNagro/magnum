package com.augustnagro.magnum

import java.sql.ResultSet
import scala.deriving.Mirror
import scala.compiletime.{
  constValue,
  constValueTuple,
  erasedValue,
  error,
  summonInline,
  summonFrom
}
import scala.compiletime.ops.any.==
import scala.compiletime.ops.boolean.&&
import scala.reflect.ClassTag

sealed trait DbConfig[EC, E, ID]:

  private[magnum] def count(using DbCon): Long
  private[magnum] def existsById(id: ID)(using DbCon): Boolean
  private[magnum] def findAll(using DbCon): Vector[E]
  private[magnum] def findAll(spec: Spec[E])(using DbCon): Vector[E]
  private[magnum] def findById(id: ID)(using DbCon): Option[E]
  private[magnum] def findAllById(ids: Iterable[ID])(using DbCon): Vector[E]
  private[magnum] def deleteById(id: ID)(using DbCon): Unit
  private[magnum] def truncate()(using DbCon): Unit
  private[magnum] def deleteAllById(ids: Iterable[ID])(using DbCon): Unit
  private[magnum] def insert(entityCreator: EC)(using DbCon): E
  private[magnum] def insertAll(entityCreators: Iterable[EC])(using
      DbCon
  ): Vector[E]
  private[magnum] def update(entity: E)(using DbCon): Unit
  private[magnum] def updateAll(entities: Iterable[E])(using DbCon): Unit

object DbConfig:

  inline def apply[EC <: Product, E <: Product, ID]()(using
      ecMirror: Mirror.ProductOf[EC],
      eMirror: Mirror.ProductOf[E],
      dbEntity: DbReader[E]
  ): DbConfig[EC, E, ID] =
    type ECMets = ecMirror.MirroredElemTypes
    type ECMels = ecMirror.MirroredElemLabels
    type ECLabel = ecMirror.MirroredLabel
    inline val ecArity = constValue[Tuple.Size[ECMets]]
    inline if ecArity == 0 then
      error(s"${constValue[ECLabel]} needs at least 1 element")

    type EMets = eMirror.MirroredElemTypes
    type EMels = eMirror.MirroredElemLabels
    type ELabel = eMirror.MirroredLabel
    inline val eArity = constValue[Tuple.Size[EMets]]
    inline if eArity == 0 then
      error(s"${constValue[ELabel]} needs at least 1 element")

    assertECIsSubsetOfE[ECMets, ECMels, EMets, EMels]

    /*
    Query parts
     */
    val tblName = constValue[ELabel]

    /*
    The queries themselves
     */
    val countSql = s"SELECT count(*) FROM $tblName"

    new DbConfig[EC, E, ID]:
      def count(using con: DbCon): Long = ???
      def existsById(id: ID)(using DbCon): Boolean = ???
      def findAll(using DbCon): Vector[E] = ???
      def findAll(spec: Spec[E])(using DbCon): Vector[E] = ???
      def findById(id: ID)(using DbCon): Option[E] = ???
      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] = ???
      def deleteById(id: ID)(using DbCon): Unit = ???
      def truncate()(using DbCon): Unit = ???
      def deleteAllById(ids: Iterable[ID])(using DbCon): Unit = ???
      def insert(entityCreator: EC)(using DbCon): E = ???
      def insertAll(entityCreators: Iterable[EC])(using DbCon): Vector[E] = ???
      def update(entity: E)(using DbCon): Unit = ???
      def updateAll(entities: Iterable[E])(using DbCon): Unit = ???

private inline def assertECIsSubsetOfE[
    ECMets <: Tuple,
    ECMels <: Tuple,
    EMets <: Tuple,
    EMels <: Tuple
]: Unit =
  inline (erasedValue[ECMets], erasedValue[ECMels]) match
    case _: (EmptyTuple, EmptyTuple) => ()
    case _: ((ecMet *: ecMetTail), (ecMel *: ecMelTail)) =>
      inline if ecFieldInE[ecMet, ecMel, EMets, EMels] then
        assertECIsSubsetOfE[ecMetTail, ecMelTail, EMets, EMels]
      else
        error(
          "EC must be a subset of E. Are there any fields on EC you forgot to update on E?"
        )

private inline def ecFieldInE[ECMet, ECMel, EMets <: Tuple, EMels <: Tuple]
    : Boolean =
  inline (erasedValue[EMets], erasedValue[EMels]) match
    case _: (EmptyTuple, EmptyTuple) => false
    case _: ((eMet *: eMetTail), (eMel *: eMelTail)) =>
      inline if testTypes[eMel, ECMel] && testTypes[eMet, ECMet] then true
      else ecFieldInE[ECMet, ECMel, eMetTail, eMelTail]

private inline def testTypes[A, B]: Boolean =
  summonFrom {
    case _: (A =:= B) => true
    case _            => false
  }

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
