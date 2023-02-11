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

sealed trait DbConfig[EC, E, ID]:

  def sqlNameMapper: SqlNameMapper
  def tableName: String

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
  private[magnum] transparent inline def cols: Any

object DbConfig:

  inline def apply[EC <: Product, E <: Product, ID](
      nameMapper: SqlNameMapper = SameCase
  )(using
      ecMirror: Mirror.ProductOf[EC],
      eMirror: Mirror.ProductOf[E],
      eDbEntity: DbEntity[E]
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

    type EMetsAndMels = Tuple.Zip[EMets, EMels]
    assertEcIsSubsetOfE[ECMets, ECMels, ECLabel, EMetsAndMels, ELabel]

    /*
    Query parts
     */
    val tblName = nameMapper.toTableName(constValue[ELabel])

    /*
    The queries themselves
     */
    val countSql = s"SELECT count(*) FROM $tblName"

    new DbConfig[EC, E, ID]:
      def tableName: String = tblName
      def sqlNameMapper: SqlNameMapper = nameMapper
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

private inline def assertEcIsSubsetOfE[
    ECMets <: Tuple,
    ECMels <: Tuple,
    ECLabel,
    EMetsAndMels <: Tuple,
    ELabel
]: Unit =
  inline (erasedValue[ECMets], erasedValue[ECMels]) match
    case _: (EmptyTuple, EmptyTuple) => ()
    case _: ((ecMet *: ecMetTail), (ecMel *: ecMelTail)) =>
      type TestEField[ETup] <: Boolean = ETup match
        case (eMet *: eMel) => (eMet == ecMet) && (eMel == ecMel)
        case _              => false

      type ECFieldIsInE =
        Tuple.Size[Tuple.Filter[EMetsAndMels, TestEField]] == 1

      inline if constValue[ECFieldIsInE] then
        assertEcIsSubsetOfE[ecMetTail, ecMelTail, ECLabel, EMetsAndMels, ELabel]
      else
        error(s"${constValue[ECLabel]} must be a subset of ${constValue[
            ELabel
          ]}. Field ${constValue[ecMel]} was not found on ${constValue[ELabel]}")
