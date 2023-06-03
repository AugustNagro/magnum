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
  def delete(entity: E)(using DbCon): Boolean
  def deleteById(id: ID)(using DbCon): Boolean
  def truncate()(using DbCon): Int
  def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult
  def deleteAllById(ids: Iterable[ID])(using DbCon): BatchUpdateResult
  def insert(entityCreator: EC)(using DbCon): Unit
  def insertAll(entityCreators: Iterable[EC])(using DbCon): Unit
  def insertReturning(entityCreator: EC)(using DbCon): E
  def insertAllReturning(entityCreators: Iterable[EC])(using DbCon): Vector[E]
  def update(entity: E)(using DbCon): Boolean
  def updateAll(entities: Iterable[E])(using DbCon): BatchUpdateResult

object RepoDefaults:

  inline given [EC: DbCodec, E: DbCodec, ID: ClassTag]
      : RepoDefaults[EC, E, ID] = ${ genImpl[EC, E, ID] }

  private def genImpl[EC: Type, E: Type, ID: Type](using
      Quotes
  ): Expr[RepoDefaults[EC, E, ID]] =
    import quotes.reflect.*
    assertECIsSubsetOfE[EC, E]

    val tableAnnot = TypeRepr.of[Table]
    val table: Expr[Table] =
      TypeRepr
        .of[E]
        .typeSymbol
        .annotations
        .collectFirst {
          case term if term.tpe =:= tableAnnot => term.asExprOf[Table]
        } match
        case Some(table) => table
        case None =>
          report.error(
            s"${TypeRepr.of[E].show} must have @Table annotation to derive Repo methods"
          )
          '{ ??? }

    val nameMapper: Expr[SqlNameMapper] = '{ $table.nameMapper }

    // todo handle None
    Expr.summon[Mirror.Of[E]] match
      case '{
            $eMirror: Mirror.Of[E] {
              type MirroredLabel = eLabel
              type MirroredElemLabels = eMels
            }
          } =>
        Expr.summon[Mirror.Of[EC]] match
          case '{
                $ecMirror: Mirror.Of[EC] {
                  type MirroredElemLabels = ecMels
                }
              } =>
            val tableName = Type.valueOfConstant[eLabel & String].get
            val tableNameSql = '{ $nameMapper.toTableName(tableName) }
            val eElemNames = elemNames[eMels]()
            val eElemNamesSql = Expr.ofSeq(
              eElemNames.map(elemName =>
                sqlNameAnnot[E](elemName) match
                  case Some(sqlName) => '{ $sqlName.name }
                  case None          => '{ $nameMapper.toColumnName(elemName) }
              )
            )
            val ecElemNames = elemNames[ecMels]()
            val ecElemNamesSql = Expr.ofSeq(
              ecElemNames.map(elemName =>
                sqlNameAnnot[E](elemName) match
                  case Some(sqlName) => '{ $sqlName.name }
                  case None          => '{ $nameMapper.toColumnName(elemName) }
              )
            )
            val idIndex = idAnnotIndex[E]
            val eCodec = Expr.summon[DbCodec[E]].get
            val ecCodec = Expr.summon[DbCodec[EC]].get
            val eClassTag = Expr.summon[ClassTag[E]].get
            val ecClassTag = Expr.summon[ClassTag[EC]].get
            val idClassTag = Expr.summon[ClassTag[Id]].get
            '{
              $table.dbType.buildRepoDefaults[EC, E, ID](
                $tableNameSql,
                eElemNames,
                $eElemNamesSql,
                ecElemNames,
                $ecElemNamesSql,
                $idIndex
              )(using $eCodec, $ecCodec, $eClassTag, $ecClassTag, $idClassTag)
            }

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

  private def elemNames[Mels: Type](res: List[String] = Nil)(using
      Quotes
  ): List[String] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val melString = Type.valueOfConstant[mel & String].get
        elemNames[melTail](melString :: res)
      case '[EmptyTuple] =>
        res.reverse

  private def sqlNameAnnot[T: Type](elemName: String)(using
      Quotes
  ): Option[Expr[SqlName]] =
    import quotes.reflect.*
    val sqlNameAnnot = TypeRepr.of[SqlName]
    TypeRepr
      .of[T]
      .typeSymbol
      .fieldMember(elemName)
      .annotations
      .find(term => term.tpe =:= sqlNameAnnot)
      .map(term => term.asExpr[SqlName])

//  private def genImplTyped[EC: Type, E: Type, ]

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
