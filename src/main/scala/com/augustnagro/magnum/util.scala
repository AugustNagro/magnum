package com.augustnagro.magnum

import com.augustnagro.magnum.SqlException
import java.lang.System.Logger.Level
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import javax.sql.DataSource
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

extension (sc: StringContext)
  def sql(args: Any*): Frag =
    if args.isEmpty then return Frag(sc.parts.mkString, Vector.empty)
    val resQuery = StringBuilder().append(sc.parts(0))
    val resParams = Vector.newBuilder[Any]
    for i <- args.indices do
      args(i) match
        case dbSchema: DbSchema[?, ?, ?] =>
          resQuery.append(dbSchema.tableWithAlias)
        case schemaName: DbSchemaName =>
          resQuery.append(schemaNameToSql(schemaName))
        case schemaNames: Array[DbSchemaName] =>
          resQuery.append(schemaNames.map(schemaNameToSql).mkString(", "))
        case param =>
          resQuery.append('?')
          resParams += param
      resQuery.append(sc.parts(i + 1))
    Frag(resQuery.result(), resParams.result())

private def schemaNameToSql(sn: DbSchemaName): String =
  if sn.tableAlias.isEmpty then sn.sqlName
  else sn.tableAlias + "." + sn.sqlName

// todo batchQuery
// todo batchUpdate
def runBatch[T, E](values: Iterable[T])(
    f: T => Frag
)(using con: DbCon, dbReader: DbCodec[E]): Vector[E] =
  if values.isEmpty then return Vector.empty
  val firstSql = f(values.head)

  Using.Manager(use =>
    val ps = use(
      con.connection
        .prepareStatement(firstSql.query, Statement.RETURN_GENERATED_KEYS)
    )
    setValues(ps, firstSql.params)
    ps.addBatch()
    for v <- values.tail do
      val sql = f(v)
      assert(
        sql.query == firstSql.query,
        "all queries must be the same for batch PreparedStatement"
      )
      setValues(ps, sql.params)
      ps.addBatch()

    ps.executeBatch()
    val genRs = use(ps.getGeneratedKeys)
    dbReader.read(genRs)
  ) match
    case Success(res) => res
    case Failure(t)   => throw SqlException(t, firstSql.query, values)

// todo change to use Codecs, make sql" a macro and store codec list in Frag
private def setValues(
    ps: PreparedStatement,
    params: Iterable[Any]
): Unit =
  var i = 1
  val it = params.iterator
  while it.hasNext do
    val javaObject = it.next() match
      case bd: scala.math.BigDecimal => bd.bigDecimal
      case bi: scala.math.BigInt     => bi.bigInteger
      case o: Option[?]              => o.orNull
      case x                         => x
    ps.setObject(i, javaObject)
    i += 1

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
