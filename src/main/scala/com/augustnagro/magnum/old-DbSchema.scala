package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
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
import scala.util.{Failure, Success, Using}

/*
trait DbSchema[EC, E, ID] extends Selectable:
  def selectDynamic(scalaName: String): DbSchemaName
  def all: IArray[DbSchemaName]
  def alias: String
  def alias(tableSqlAlias: String): this.type

  private[magnum] def tableWithAlias: String
  private[magnum] def count(using DbCon): Long
  private[magnum] def existsById(id: ID)(using DbCon): Boolean
  private[magnum] def findAll(using DbCon): Vector[E]
  private[magnum] def findAll(spec: Spec[E])(using DbCon): Vector[E]
  private[magnum] def findById(id: ID)(using DbCon): Option[E]
  private[magnum] def findAllById(ids: Iterable[ID])(using DbCon): Vector[E]
  private[magnum] def delete(entity: E)(using DbCon): Boolean
  private[magnum] def deleteById(id: ID)(using DbCon): Boolean
  private[magnum] def truncate()(using DbCon): Int
  private[magnum] def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult
  private[magnum] def deleteAllById(ids: Iterable[ID])(using DbCon): BatchUpdateResult
  private[magnum] def insert(entityCreator: EC)(using DbCon): Unit
  private[magnum] def insertAll(entityCreators: Iterable[EC])(using
      DbCon
  ): Unit
  private[magnum] def insertReturning(entityCreator: EC)(using DbCon): E
  private[magnum] def insertAllReturning(entityCreators: Iterable[EC])(using
      DbCon
  ): Vector[E]
  private[magnum] def update(entity: E)(using DbCon): Boolean
  private[magnum] def updateAll(entities: Iterable[E])(using DbCon): BatchUpdateResult

object DbSchema:
  val DefaultAlias = ""

  transparent inline def apply[EC <: Product, E <: Product, ID](
      dbType: DbType,
      sqlNameMapper: SqlNameMapper = SqlNameMapper.SameCase
  )(using
    Mirror.ProductOf[EC],
    Mirror.ProductOf[E],
    DbCodec[E],
    ClassTag[ID]
  ) = ${ dbSchemaImpl[EC, E, ID]('{ dbType }, '{ sqlNameMapper }) }

  private def assertECIsSubsetOfE[EC: Type, E: Type](using Quotes): Unit =
    import quotes.reflect.*
    val eRepr = TypeRepr.of[E]
    val ecRepr = TypeRepr.of[EC]
    val eFields = eRepr.typeSymbol.caseFields
    val ecFields = ecRepr.typeSymbol.caseFields

    for ecField <- ecFields do
      if !eFields.exists(f =>
          f.name == ecField.name &&
            f.signature.resultSig == ecField.signature.resultSig
        )
      then
        report.error(
          s"""${ecRepr.show} must be an effective subset of ${eRepr.show}.
           |Are there any fields on ${ecRepr.show} you forgot to update on ${eRepr.show}?
           |""".stripMargin
        )

  private def dbSchemaImpl[EC: Type, E: Type, ID: Type](
      dbType: Expr[DbType],
      sqlNameMapper: Expr[SqlNameMapper]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    assertECIsSubsetOfE[EC, E]
    Expr.summon[Mirror.ProductOf[E]].get match
      case '{
            $m: Mirror.ProductOf[E] {
              type MirroredElemLabels = mels
              type MirroredLabel = tableName
            }
          } =>
        val tableName = Expr(Type.valueOfConstant[tableName].get.toString)
        val tableNameSql = '{ $sqlNameMapper.toTableName($tableName) }
        applyRefinements[EC, E, ID, mels, DbSchema[EC, E, ID]](
          sqlNameMapper,
          tableNameSql,
          dbType,
          Nil
        )

  private def applyRefinements[
      EC: Type,
      E: Type,
      ID: Type,
      EMels: Type,
      RES: Type
  ](
      sqlNameMapper: Expr[SqlNameMapper],
      tableNameSql: Expr[String],
      dbType: Expr[DbType],
      fieldNames: List[String]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    Type.of[EMels] match
      case '[mel *: melTail] =>
        val scalaFieldName = Type.valueOfConstant[mel].get.toString
        val fieldNameRefinement =
          Refinement(
            TypeRepr.of[RES],
            scalaFieldName,
            TypeRepr.of[DbSchemaName]
          )
        fieldNameRefinement.asType match
          case '[tpe] =>
            applyRefinements[EC, E, ID, melTail, tpe](
              sqlNameMapper,
              tableNameSql,
              dbType,
              scalaFieldName :: fieldNames
            )
      case '[EmptyTuple] =>
        buildDbSchema[EC, E, ID, RES](
          tableNameSql,
          Expr(fieldNames.reverse),
          ecFieldNames[EC],
          sqlNameMapper,
          dbType
        )

  private def ecFieldNames[EC: Type](using Quotes): Expr[List[String]] =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[EC]].get match
      case '{
            $m: Mirror.ProductOf[EC] {
              type MirroredElemLabels = mels
            }
          } =>
        ecFieldNamesImpl[mels](Nil)

  private def ecFieldNamesImpl[Mels: Type](
      res: List[String]
  )(using Quotes): Expr[List[String]] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val fieldName = Type.valueOfConstant[mel].get.toString
        ecFieldNamesImpl[melTail](fieldName :: res)
      case '[EmptyTuple] =>
        Expr(res.reverse)

  private def idAnnotIndex[E: Type](using Quotes): Expr[Int] =
    import quotes.reflect.*
    val idAnnot = TypeRepr.of[Id]
    val index = TypeRepr
      .of[E]
      .typeSymbol
      .caseFields
      .indexWhere(sym =>
        sym.annotations.exists(term => term.tpe =:= idAnnot)
      ) match
      case -1 => 0
      case x  => x
    Expr(index)

  private def buildDbSchema[
      EC: Type,
      E: Type,
      ID: Type,
      RES: Type
  ](
      tableNameSql: Expr[String],
      fieldNames: Expr[List[String]],
      ecFieldNames: Expr[List[String]],
      sqlNameMapper: Expr[SqlNameMapper],
      dbType: Expr[DbType]
  )(using Quotes): Expr[Any] =
    val dbReader = Expr.summon[DbCodec[E]].get
    val eClassTag = Expr.summon[ClassTag[E]].get
    val ecClassTag = Expr.summon[ClassTag[EC]].get
    val idClassTag = Expr.summon[ClassTag[ID]].get
    val eMirror = Expr.summon[Mirror.ProductOf[E]].get
    '{
      $dbType.buildDbSchema[EC, E, ID, RES](
        $tableNameSql,
        $fieldNames,
        $ecFieldNames,
        $sqlNameMapper,
        ${ idAnnotIndex[E] }
      )(using $dbReader, $ecClassTag, $eClassTag, $idClassTag, $eMirror)
    }

 */
