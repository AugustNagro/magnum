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
  val stringExprs: Seq[Expr[String]] = sc match
    case '{ StringContext(${ Varargs(strings) }: _*) } => strings

  val paramsExpr = Expr.ofSeq(argsExprs)
  val questionVarargs = Varargs(Vector.fill(argsExprs.size)(Expr("?")))
  val queryExpr = '{ $sc.s($questionVarargs: _*) }

  '{
    val argValues = $args
    val writer = (ps: PreparedStatement, pos: Int) => {
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
)(using Quotes): Expr[Unit] =
  import quotes.reflect.*
  argsExprs match
    case head +: tail =>
      head match
        case '{ $arg: tp } =>
          Expr.summon[DbCodec[tp]] match
            case Some(codecExpr) =>
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
            case None =>
              report.info(
                s"Could not find given DbCodec for ${TypeRepr.of[tp].show}. Using PreparedStatement::setObject instead."
              )
              '{
                val i = $iExpr
                val argValue = $args(i)
                val pos = $posExpr
                $psExpr.setObject(pos, argValue)
                val newPos = pos + 1
                val newI = i + 1
                ${ sqlWriter(psExpr, '{ newPos }, args, tail, '{ newI }) }
              }
        case _ =>
          report.error("Args must be explicit", head)
          '{ ??? }
    case Seq() => '{}

//    if args.isEmpty then return Frag(sc.parts.mkString, Vector.empty)
//    val resQuery = StringBuilder().append(sc.parts(0))
//    val resParams = Vector.newBuilder[Any]
//    for i <- args.indices do
//      args(i) match
//        case param =>
//          resQuery.append('?')
//          resParams += param
//      resQuery.append(sc.parts(i + 1))
//    Frag(resQuery.result(), resParams.result())

// todo batchQuery
// todo batchUpdate
def runBatch[T, E](values: Iterable[T])(
    f: T => Frag
)(using con: DbCon, dbReader: DbCodec[E]): Vector[E] =
  if values.isEmpty then return Vector.empty
  val firstFrag = f(values.head)

  Using.Manager(use =>
    val ps = use(
      con.connection
        .prepareStatement(firstFrag.query, Statement.RETURN_GENERATED_KEYS)
    )
    firstFrag.writer(ps, 0)
    ps.addBatch()
    for v <- values.tail do
      val frag = f(v)
      assert(
        frag.query == firstFrag.query,
        "all queries must be the same for batch PreparedStatement"
      )
      frag.writer(ps, 0)
      ps.addBatch()

    ps.executeBatch()
    val genRs = use(ps.getGeneratedKeys)
    dbReader.read(genRs)
  ) match
    case Success(res) => res
    case Failure(t)   => throw SqlException(firstFrag, t)

// todo change to use Codecs, make sql" a macro and store codec list in Frag
//private def setValues(
//    ps: PreparedStatement,
//    params: Iterable[Any]
//): Unit =
//  var i = 1
//  val it = params.iterator
//  while it.hasNext do
//    val javaObject = it.next() match
//      case bd: scala.math.BigDecimal => bd.bigDecimal
//      case bi: scala.math.BigInt     => bi.bigInteger
//      case o: Option[?]              => o.orNull
//      case x                         => x
//    ps.setObject(i, javaObject)
//    i += 1

private def logSql(sql: Frag): Unit = logSql(sql.query, sql.params)

private val Log = System.getLogger(getClass.getName)

private def logSql(query: String, params: Iterable[Any]): Unit =
  if Log.isLoggable(Level.TRACE) then
    val paramsString = params.mkString("[", ", ", "]")
    Log.log(
      Level.TRACE,
      s"""Executing Query:
         |$query
         |
         |With values:
         |$paramsString""".stripMargin
    )
  else if Log.isLoggable(Level.DEBUG) then
    Log.log(
      Level.DEBUG,
      s"""Executing Query:
         |$query""".stripMargin
    )

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
