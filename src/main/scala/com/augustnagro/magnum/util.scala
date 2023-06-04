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
  import quotes.reflect.report
  val argsExprs: Seq[Expr[Any]] = args match
    case Varargs(ae) => ae
//  val stringExprs: Seq[Expr[String]] = sc match
//    case '{ StringContext(${ Varargs(strings) }: _*) } => strings

  val paramsExpr = Expr.ofSeq(argsExprs)
  val questionVarargs = Varargs(Vector.fill(argsExprs.size)(Expr("?")))
  val queryExpr = '{ $sc.s($questionVarargs: _*) }

  '{
    val argValues = $args
    val writer: FragWriter = (ps: PreparedStatement, pos: Int) => {
      ${ sqlWriter('{ ps }, '{ pos }, '{ argValues }, argsExprs, '{ 0 }) }
    }
    Frag($queryExpr, argValues, writer)
  }

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
          report.error("Args must be explicit", head)
          '{ ??? }
    case Seq() => posExpr

//private def summonWriters(
//    argsExprs: Seq[Expr[Any]],
//    res: Vector[Expr[DbCodec[Any]]] = Vector.empty
//)(using Quotes): Expr[Seq[DbCodec[Any]]] =
//  import quotes.reflect.*
//  argsExprs match
//    case head +: tail =>
//      head match
//        case '{ $arg: tp } =>
//          val writer = summonWriter[tp]
//          summonWriters(tail, res :+ writer)
//        case _ =>
//          report.error("Args must be explicit", head)
//          '{ ??? }
//    case Seq() => Expr.ofSeq(res)

private def summonWriter[T: Type](using Quotes): Expr[DbCodec[T]] =
  import quotes.reflect.*
  TypeRepr
    .of[T]
    .baseClasses
    .view
    .init // remove the 'Any' class at the end of the list, since we want to warn if using DbCodec[Any]
    .flatMap(symbol =>
      symbol.typeRef.asType match
        case '[tpe] => Expr.summon[DbCodec[tpe]]
    )
    .map(codec => '{ $codec.asInstanceOf[DbCodec[T]] })
    .headOption
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

private def logSql(sql: Frag): Unit = logSql(sql.sqlString, sql.params)

private val Log = System.getLogger(getClass.getName)

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
