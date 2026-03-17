package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}
import zio.{Task, Trace, UIO, URLayer, ZIO, ZLayer}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

class TransactorZIO private (
    dataSource: DataSource,
    sqlLogger: SqlLogger,
    connectionConfig: Connection => Unit
):

  def withSqlLogger(sqlLogger: SqlLogger): TransactorZIO =
    new TransactorZIO(
      dataSource,
      sqlLogger,
      connectionConfig
    )

  def withConnectionConfig(
      connectionConfig: Connection => Unit
  ): TransactorZIO =
    new TransactorZIO(
      dataSource,
      sqlLogger,
      connectionConfig
    )

  def connect[A](f: DbCon ?=> A)(using Trace): Task[A] =
    ZIO.blocking(
      ZIO.acquireReleaseWith(acquireConnection)(releaseConnection)(cn =>
        ZIO.attempt {
          connectionConfig(cn)
          f(using DbCon(cn, sqlLogger))
        }
      )
    )

  def transact[A](f: DbTx ?=> A)(using Trace): Task[A] =
    ZIO.blocking(
      ZIO.acquireReleaseWith(acquireConnection)(releaseConnection)(cn =>
        ZIO.attempt {
          connectionConfig(cn)
          cn.setAutoCommit(false)
          try
            val res = f(using DbTx(cn, sqlLogger))
            cn.commit()
            res
          catch
            case NonFatal(t) =>
              try cn.rollback()
              catch { case t2 => t.addSuppressed(t2) }
              throw t
        }.uninterruptible
      )
    )

  private def acquireConnection(using Trace): Task[Connection] =
    ZIO
      .attempt(dataSource.getConnection())
      .mapError(t => SqlException("Unable to acquire DB Connection", t))

  private def releaseConnection(con: Connection)(using Trace): UIO[Unit] =
    if con eq null then ZIO.unit
    else
      ZIO
        .attempt(con.close())
        .orDieWith(t =>
          SqlException("Unable to close DB Connection, will die", t)
        )
end TransactorZIO

object TransactorZIO:
  private val noOpConnectionConfig: Connection => Unit = _ => ()

  /** Construct a TransactorZIO
    *
    * @param sqlLogger
    *   Logging configuration
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    */
  def layer(
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): URLayer[DataSource, TransactorZIO] =
    ZLayer
      .service[DataSource]
      .project(ds =>
        TransactorZIO(
          dataSource = ds,
          sqlLogger = sqlLogger,
          connectionConfig = connectionConfig
        )
      )

  /** Construct a TransactorZIO
    *
    * @param sqlLogger
    *   Logging configuration
    */
  def layer(sqlLogger: SqlLogger): URLayer[DataSource, TransactorZIO] =
    layer(
      sqlLogger = sqlLogger,
      connectionConfig = noOpConnectionConfig
    )

  /** Construct a TransactorZIO */
  def layer: URLayer[DataSource, TransactorZIO] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = noOpConnectionConfig
    )

  /** Construct a TransactorZIO
    *
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    */
  def layer(
      connectionConfig: Connection => Unit
  ): URLayer[DataSource, TransactorZIO] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = connectionConfig
    )

end TransactorZIO
