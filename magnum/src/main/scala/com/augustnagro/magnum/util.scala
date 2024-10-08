package com.augustnagro.magnum

import com.augustnagro.magnum.SqlException

import java.lang.System.Logger.Level
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import javax.sql.DataSource
import scala.collection.mutable.ReusableBuilder
import scala.util.{Failure, Success, Using, boundary}
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

def connect[T](dataSource: DataSource)(f: DbCon ?=> T): T =
  Using.resource(dataSource.getConnection)(con => f(using DbCon(con)))

def transact[T](dataSource: DataSource)(f: DbTx ?=> T): T =
  Using.resource(dataSource.getConnection)(con =>
    con.setAutoCommit(false)
    try
      val res = f(using DbTx(con))
      con.commit()
      res
    catch
      case t =>
        con.rollback()
        throw t
  )

def transact[T](dataSource: DataSource, connectionConfig: Connection => Unit)(
    f: DbTx ?=> T
): T =
  Using.resource(dataSource.getConnection)(con =>
    connectionConfig(con)
    con.setAutoCommit(false)
    try
      val res = f(using DbTx(con))
      con.commit()
      res
    catch
      case t =>
        con.rollback()
        throw t
  )

extension (inline sc: StringContext)
  inline def sql(inline args: Any*): Frag =
    ${ sqlImpl('{ sc }, '{ args }) }

private def sqlImpl(sc: Expr[StringContext], args: Expr[Seq[Any]])(using
    Quotes
): Expr[Frag] =
  import quotes.reflect.*
  val argsExprs: Seq[Expr[Any]] = args match
    case Varargs(ae) => ae
//  val stringExprs: Seq[Expr[String]] = sc match
//    case '{ StringContext(${ Varargs(strings) }: _*) } => strings

  val interpolatedVarargs = Varargs(argsExprs.map {
    case '{ $arg: SqlLiteral } => '{ $arg.queryRepr }
    case '{ $arg: tp } =>
      val codecExpr = summonWriter[tp]
      '{ $codecExpr.queryRepr }
  })

  val paramExprs = argsExprs.filter {
    case '{ $arg: SqlLiteral } => false
    case _                     => true
  }

  val queryExpr = '{ $sc.s($interpolatedVarargs: _*) }
  val exprParams = Expr.ofSeq(paramExprs)

  '{
    val argValues = $exprParams
    val writer: FragWriter = (ps: PreparedStatement, pos: Int) => {
      ${ sqlWriter('{ ps }, '{ pos }, '{ argValues }, paramExprs, '{ 0 }) }
    }
    Frag($queryExpr, argValues, writer)
  }
end sqlImpl

private def sqlWriter(
    psExpr: Expr[PreparedStatement],
    posExpr: Expr[Int],
    args: Expr[Seq[Any]],
    argsExprs: Seq[Expr[Any]],
    iExpr: Expr[Int]
)(using Quotes): Expr[Int] =
  import quotes.reflect.*
  argsExprs match
    case head +: tail =>
      head match
        case '{ $arg: tp } =>
          val codecExpr = summonWriter[tp]
          '{
            val i = $iExpr
            val argValue = $args(i).asInstanceOf[tp]
            val pos = $posExpr
            val codec = $codecExpr
            codec.writeSingle(argValue, $psExpr, pos)
            val newPos = pos + codec.cols.length
            val newI = i + 1
            ${ sqlWriter(psExpr, '{ newPos }, args, tail, '{ newI }) }
          }
        case _ =>
          report.errorAndAbort("Args must be explicit", head)
    case Seq() => posExpr
end sqlWriter

private def summonWriter[T: Type](using Quotes): Expr[DbCodec[T]] =
  import quotes.reflect.*

  Expr
    .summon[DbCodec[T]]
    .orElse(
      TypeRepr.of[T].widen.asType match
        case '[tpe] =>
          Expr
            .summon[DbCodec[tpe]]
            .map(codec => '{ $codec.asInstanceOf[DbCodec[T]] })
    )
    .getOrElse:
      report.info(
        s"Could not find given DbCodec for ${TypeRepr.of[T].show}. Using PreparedStatement::setObject instead."
      )
      '{ DbCodec.AnyCodec.asInstanceOf[DbCodec[T]] }

def batchUpdate[T](values: Iterable[T])(f: T => Update)(using
    con: DbCon
): BatchUpdateResult =
  val it = values.iterator
  if !it.hasNext then return BatchUpdateResult.Success(0)
  val firstUpdate = f(it.next())
  val firstFrag = firstUpdate.frag

  Using.Manager(use =>
    val ps = use(con.connection.prepareStatement(firstFrag.sqlString))
    firstFrag.writer.write(ps, 1)
    ps.addBatch()

    while it.hasNext do
      val frag = f(it.next()).frag
      assert(
        frag.sqlString == firstFrag.sqlString,
        "all queries must be the same for batch PreparedStatement"
      )
      frag.writer.write(ps, 1)
      ps.addBatch()
    batchUpdateResult(ps.executeBatch())
  ) match
    case Success(res) => res
    case Failure(ex)  => throw SqlException(firstFrag, ex)
end batchUpdate

private def logSql(sql: Frag): Unit = logSql(sql.sqlString, sql.params)

private val Log = System.getLogger("com.augustnagro.magnum")

private def logSql(query: String, params: Any): Unit =
  if Log.isLoggable(Level.TRACE) then
    Log.log(
      Level.TRACE,
      s"""Executing Query:
         |$query
         |
         |With values:
         |${logSqlParams(params)}""".stripMargin
    )
  else if Log.isLoggable(Level.DEBUG) then
    Log.log(
      Level.DEBUG,
      s"""Executing Query:
         |$query""".stripMargin
    )

private def logSqlParams(params: Any): String =
  params match
    case p: Product => p.productIterator.mkString("(", ", ", ")")
    case it: Iterable[?] =>
      it.headOption match
        case Some(h: Product) =>
          it.asInstanceOf[Iterable[Product]]
            .map(_.productIterator.mkString("(", ", ", ")"))
            .mkString("[\n", ",\n", "]\n")
        case _ =>
          it.mkString("(", ", ", ")")
    case x => x.toString

private def batchUpdateResult(updateCounts: Array[Int]): BatchUpdateResult =
  boundary:
    val updatedRows = updateCounts.foldLeft(0L)((res, c) =>
      c match
        case rowCount if rowCount >= 0 =>
          res + rowCount
        case Statement.SUCCESS_NO_INFO =>
          boundary.break(BatchUpdateResult.SuccessNoInfo)
        case errorCode =>
          throw RuntimeException(s"Received JDBC error code $errorCode")
    )
    BatchUpdateResult.Success(updatedRows)

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

private def tableExprs[EC: Type, E: Type, ID: Type](using
    Quotes
): TableExprs =
  import quotes.reflect.*
  assertECIsSubsetOfE[EC, E]

  val idIndex = idAnnotIndex[E]
  val table: Expr[Table] =
    DerivingUtil.tableAnnot[E] match
      case Some(table) => table
      case None =>
        report.errorAndAbort(
          s"${TypeRepr.of[E].show} must have @Table annotation"
        )
  val nameMapper: Expr[SqlNameMapper] = '{ $table.nameMapper }

  Expr.summon[Mirror.Of[E]] match
    case Some('{
          $eMirror: Mirror.Of[E] {
            type MirroredLabel = eLabel
            type MirroredElemLabels = eMels
          }
        }) =>
      Expr.summon[Mirror.Of[EC]] match
        case Some('{
              $ecMirror: Mirror.Of[EC] {
                type MirroredElemLabels = ecMels
              }
            }) =>
          val tableNameScala = Type.valueOfConstant[eLabel].get.toString
          val tableNameScalaExpr = Expr(tableNameScala)
          val tableNameSql = DerivingUtil.sqlTableNameAnnot[E] match
            case Some(sqlName) => '{ $sqlName.name }
            case None => '{ $nameMapper.toTableName($tableNameScalaExpr) }
          val eElemNames = elemNames[eMels]()
          val eElemNamesSql = eElemNames.map(elemName =>
            sqlNameAnnot[E](elemName) match
              case Some(sqlName) => '{ $sqlName.name }
              case None =>
                '{ $nameMapper.toColumnName(${ Expr(elemName) }) }
          )
          val ecElemNames = elemNames[ecMels]()
          val ecElemNamesSql = ecElemNames.map(elemName =>
            sqlNameAnnot[E](elemName) match
              case Some(sqlName) => '{ $sqlName.name }
              case None =>
                '{ $nameMapper.toColumnName(${ Expr(elemName) }) }
          )
          TableExprs(
            table,
            tableNameScalaExpr,
            tableNameSql,
            eElemNames,
            eElemNamesSql,
            ecElemNames,
            ecElemNamesSql,
            idIndex
          )
        case _ =>
          report.errorAndAbort(
            s"A Mirror is required to derive RepoDefaults for ${TypeRepr.of[EC].show}"
          )
    case _ =>
      report.errorAndAbort(
        s"A Mirror is required to derive RepoDefaults for ${TypeRepr.of[E].show}"
      )
  end match
end tableExprs

private def idAnnotIndex[E: Type](using q: Quotes): Expr[Int] =
  import q.reflect.*
  val idAnnot = TypeRepr.of[Id].typeSymbol
  val index = TypeRepr
    .of[E]
    .typeSymbol
    .primaryConstructor
    .paramSymss
    .head
    .indexWhere(sym => sym.hasAnnotation(idAnnot)) match
    case -1 => 0
    case x  => x
  Expr(index)

private def elemNames[Mels: Type](res: List[String] = Nil)(using
    Quotes
): List[String] =
  import quotes.reflect.*
  Type.of[Mels] match
    case '[mel *: melTail] =>
      val melString = Type.valueOfConstant[mel].get.toString
      elemNames[melTail](melString :: res)
    case '[EmptyTuple] =>
      res.reverse

private def sqlNameAnnot[T: Type](elemName: String)(using
    Quotes
): Option[Expr[SqlName]] =
  import quotes.reflect.*
  val annot = TypeRepr.of[SqlName].typeSymbol
  TypeRepr
    .of[T]
    .typeSymbol
    .primaryConstructor
    .paramSymss
    .head
    .find(sym => sym.name == elemName && sym.hasAnnotation(annot))
    .flatMap(sym => sym.getAnnotation(annot))
    .map(term => term.asExprOf[SqlName])
