package com.augustnagro.magnum

import com.augustnagro.magnum.batch.BatchUpdateResult
import com.augustnagro.magnum.builders.{
  DbCon,
  DbTx,
  Frag,
  FragWriter,
  SqlLiteral,
  Transactor,
  Update
}
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.errors.SqlException
import com.augustnagro.magnum.impl.{DerivingUtil, TableExprs}
import com.augustnagro.magnum.logging.{SqlExceptionEvent, SqlSuccessEvent}
import com.augustnagro.magnum.repo.{Id, SqlName, SqlNameMapper, Table}

import java.lang.System.Logger.Level
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.StringJoiner
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import scala.collection.mutable as m
import scala.util.{Failure, Success, Try, Using, boundary}
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
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.quoted.*

def connect[T](transactor: Transactor)(f: DbCon ?=> T): T =
  Using.resource(transactor.dataSource.getConnection): con =>
    transactor.connectionConfig(con)
    f(using DbCon(con, transactor.sqlLogger))

def connect[T](dataSource: DataSource)(f: DbCon ?=> T): T =
  connect(Transactor(dataSource))(f)

def transact[T](transactor: Transactor)(f: DbTx ?=> T): T =
  Using.resource(transactor.dataSource.getConnection): con =>
    transactor.connectionConfig(con)
    con.setAutoCommit(false)
    try
      val res = f(using DbTx(con, transactor.sqlLogger))
      con.commit()
      res
    catch
      case t =>
        con.rollback()
        throw t

def transact[T](dataSource: DataSource)(f: DbTx ?=> T): T =
  transact(Transactor(dataSource))(f)

def transact[T](dataSource: DataSource, connectionConfig: Connection => Unit)(
    f: DbTx ?=> T
): T =
  val transactor =
    Transactor(dataSource = dataSource, connectionConfig = connectionConfig)
  transact(transactor)(f)

extension (inline sc: StringContext)
  inline def sql(inline args: Any*): Frag =
    ${ sqlImpl('{ sc }, '{ args }) }

private def sqlImpl(sc: Expr[StringContext], args: Expr[Seq[Any]])(using
    Quotes
): Expr[Frag] =
  import quotes.reflect.*
  val allArgsExprs: Seq[Expr[Any]] = args match
    case Varargs(ae) => ae
//  val stringExprs: Seq[Expr[String]] = sc match
//    case '{ StringContext(${ Varargs(strings) }: _*) } => strings

  '{
    val args: Seq[Any] = ${ Expr.ofSeq(allArgsExprs) }

    val sqlQueryReprs: Vector[String] = ${
      queryReprs(allArgsExprs, '{ args }, '{ Vector.newBuilder })
    }
    val queryExpr: String = $sc.s(sqlQueryReprs: _*)

    val flattenedArgs: Vector[Any] = ${
      flattenedArgsExpr(allArgsExprs, '{ args }, '{ Vector.newBuilder })
    }

    val writer: FragWriter = (ps: PreparedStatement, pos: Int) => {
      ${ sqlWriter('{ ps }, '{ pos }, '{ args }, allArgsExprs) }
    }
    Frag(queryExpr, flattenedArgs, writer)
  }
end sqlImpl

private def flattenedArgsExpr(
    argsExprs: Seq[Expr[Any]],
    allArgs: Expr[Seq[Any]],
    builder: Expr[m.Builder[Any, Vector[Any]]],
    i: Int = 0
)(using Quotes): Expr[Vector[Any]] =
  argsExprs match
    case '{ $arg: SqlLiteral } +: tail =>
      flattenedArgsExpr(tail, allArgs, builder, i + 1)
    case '{ $arg: Frag } +: tail =>
      val newBuilder = '{
        $builder ++= $allArgs(${ Expr(i) }).asInstanceOf[Frag].params
      }
      flattenedArgsExpr(tail, allArgs, newBuilder, i + 1)
    case '{ $arg: tp } +: tail =>
      val newBuilder = '{ $builder += $allArgs(${ Expr(i) }) }
      flattenedArgsExpr(tail, allArgs, newBuilder, i + 1)
    case Seq() =>
      '{ $builder.result() }

private def queryReprs(
    argsExprs: Seq[Expr[Any]],
    allArgs: Expr[Seq[Any]],
    builder: Expr[m.Builder[String, Vector[String]]],
    i: Int = 0
)(using Quotes): Expr[Vector[String]] =
  argsExprs match
    case '{ $arg: SqlLiteral } +: tail =>
      val newBuilder = '{
        $builder += $allArgs(${ Expr(i) }).asInstanceOf[SqlLiteral].queryRepr
      }
      queryReprs(tail, allArgs, newBuilder, i + 1)
    case '{ $arg: Frag } +: tail =>
      val newBuilder = '{
        $builder += $allArgs(${ Expr(i) }).asInstanceOf[Frag].sqlString
      }
      queryReprs(tail, allArgs, newBuilder, i + 1)
    case '{ $arg: tp } +: tail =>
      val codecExpr = summonWriter[tp]
      val newBuilder = '{ $builder += $codecExpr.queryRepr }
      queryReprs(tail, allArgs, newBuilder, i + 1)
    case Seq() =>
      '{ $builder.result() }

private def sqlWriter(
    psExpr: Expr[PreparedStatement],
    posExpr: Expr[Int],
    args: Expr[Seq[Any]],
    argsExprs: Seq[Expr[Any]],
    i: Int = 0
)(using Quotes): Expr[Int] =
  import quotes.reflect.*
  argsExprs match
    case '{ $arg: SqlLiteral } +: tail =>
      sqlWriter(psExpr, posExpr, args, tail, i + 1)
    case '{ $arg: Frag } +: tail =>
      '{
        val frag = $args(${ Expr(i) }).asInstanceOf[Frag]
        val pos = $posExpr
        val newPos = frag.writer.write($psExpr, pos)
        ${ sqlWriter(psExpr, '{ newPos }, args, tail, i + 1) }
      }
    case '{ $arg: tp } +: tail =>
      val codecExpr = summonWriter[tp]
      '{
        val argValue = $args(${ Expr(i) }).asInstanceOf[tp]
        val pos = $posExpr
        val codec = $codecExpr
        codec.writeSingle(argValue, $psExpr, pos)
        val newPos = pos + codec.cols.length
        ${ sqlWriter(psExpr, '{ newPos }, args, tail, i + 1) }
      }
    case Seq() => posExpr
  end match
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
    case Failure(t) =>
      throw SqlException(
        con.sqlLogger.exceptionMsg(
          SqlExceptionEvent(firstFrag.sqlString, firstFrag.params, t)
        ),
        t
      )
  end match
end batchUpdate

private val Log = System.getLogger("com.augustnagro.magnum")

private def parseParams(params: Any): Iterator[Iterator[Any]] =
  params match
    case p: Product => Iterator(p.productIterator)
    case it: Iterable[?] =>
      it.headOption match
        case Some(h: Product) =>
          it.asInstanceOf[Iterable[Product]]
            .iterator
            .map(_.productIterator)
        case _ =>
          Iterator(it.iterator)
    case x => Iterator(Iterator(x))

private def paramsString(params: Iterator[Iterator[Any]]): String =
  params.map(_.mkString("(", ", ", ")")).mkString("", ",\n", "\n")

private def timed[T](f: => T): (T, FiniteDuration) =
  val start = System.currentTimeMillis()
  val res = f
  val execTime = FiniteDuration(
    System.currentTimeMillis() - start,
    TimeUnit.MILLISECONDS
  )
  (res, execTime)

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

private def handleQuery[A](sql: String, params: Any)(
    attempt: Try[(A, FiniteDuration)]
)(using con: DbCon): A =
  attempt match
    case Success((res, execTime)) =>
      con.sqlLogger.log(SqlSuccessEvent(sql, params, execTime))
      res
    case Failure(t) =>
      val msg = con.sqlLogger.exceptionMsg(SqlExceptionEvent(sql, params, t))
      throw SqlException(msg, t)
