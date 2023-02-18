package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet}
import javax.sql.DataSource
import scala.util.Using
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
  Using
    .Manager(manager =>
      val con = manager(dataSource.getConnection)
      f(using DbCon(con, manager))
    )
    .get

def transact[T](dataSource: DataSource)(f: DbTx ?=> T): T =
  Using
    .Manager(manager =>
      val con = manager(dataSource.getConnection)
      con.setAutoCommit(false)
      try
        val res = f(using DbTx(con, manager))
        con.commit()
        res
      catch
        case t =>
          con.rollback()
          throw t
    )
    .get

def transact[T](dataSource: DataSource, connectionConfig: Connection => Unit)(
    f: DbTx ?=> T
): T =
  Using
    .Manager(manager =>
      val con = manager(dataSource.getConnection)
      connectionConfig(con)
      con.setAutoCommit(false)
      try
        val res = f(using DbTx(con, manager))
        con.commit()
        res
      catch
        case t =>
          con.rollback()
          throw t
    )
    .get

// todo can be a macro
extension (sc: StringContext)
  def sql(args: Any*): Sql =
    if args.isEmpty then return Sql(sc.parts.mkString, Vector.empty)
    val resQuery = StringBuilder().append(sc.parts(0))
    val resParams = Vector.newBuilder[Any]
    for i <- 0 until args.length do
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
    Sql(resQuery.result(), resParams.result())

private def schemaNameToSql(sn: DbSchemaName): String =
  if sn.tableAlias.isEmpty then sn.sqlName
  else sn.tableAlias + "." + sn.sqlName

// todo
def runPreparedBatch[T](values: Iterable[T])(f: T => Sql): Unit =
  ???

def runBatch[T](values: Iterable[T])(f: T => Sql): Unit =
  ???

private[magnum] def setValues(
    ps: PreparedStatement,
    params: Vector[Any]
): Unit =
  for (p, i) <- params.iterator.zipWithIndex do
    val javaObject = p match
      case bd: scala.math.BigDecimal => bd.bigDecimal
      case bi: scala.math.BigInt     => bi.bigInteger
      case o: Option[?]              => o.orNull
      case x                         => x
    ps.setObject(i + 1, javaObject)

private[magnum] inline def getFromRow[Met](
    rs: ResultSet,
    columnIndex: Int
): Any =
  inline erasedValue[Met] match
    case _: String                => rs.getString(columnIndex)
    case _: Boolean               => rs.getBoolean(columnIndex)
    case _: Byte                  => rs.getByte(columnIndex)
    case _: Short                 => rs.getShort(columnIndex)
    case _: Int                   => rs.getInt(columnIndex)
    case _: Long                  => rs.getLong(columnIndex)
    case _: Float                 => rs.getFloat(columnIndex)
    case _: Double                => rs.getDouble(columnIndex)
    case _: Array[Byte]           => rs.getBytes(columnIndex)
    case _: java.sql.Date         => rs.getDate(columnIndex)
    case _: java.sql.Time         => rs.getTime(columnIndex)
    case _: java.sql.Timestamp    => rs.getTimestamp(columnIndex)
    case _: java.sql.Ref          => rs.getRef(columnIndex)
    case _: java.sql.Blob         => rs.getBlob(columnIndex)
    case _: java.sql.Clob         => rs.getClob(columnIndex)
    case _: java.net.URL          => rs.getURL(columnIndex)
    case _: java.sql.RowId        => rs.getRowId(columnIndex)
    case _: java.sql.NClob        => rs.getNClob(columnIndex)
    case _: java.sql.SQLXML       => rs.getSQLXML(columnIndex)
    case _: scala.math.BigDecimal => BigDecimal(rs.getBigDecimal(columnIndex))
    case _: java.math.BigDecimal  => rs.getBigDecimal(columnIndex)
    case _: scala.math.BigInt =>
      scala.math.BigInt(
        rs.getObject(columnIndex, classOf[java.math.BigInteger])
      )
    case _: Option[t] => Option(getFromRow[t](rs, columnIndex))
    case _ =>
      rs.getObject(columnIndex, summonInline[ClassTag[Met]].runtimeClass)
