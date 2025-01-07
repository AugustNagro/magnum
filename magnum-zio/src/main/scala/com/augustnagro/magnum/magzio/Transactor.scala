package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}
import zio.{Semaphore, Task, Trace, UIO, ULayer, Unsafe, ZIO, ZLayer}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

class Transactor private (
    dataSource: DataSource,
    sqlLogger: SqlLogger,
    connectionConfig: Connection => Unit,
    semaphore: Option[Semaphore]
):

  def withSqlLogger(sqlLogger: SqlLogger): Transactor =
    new Transactor(
      dataSource,
      sqlLogger,
      connectionConfig,
      semaphore
    )

  def withConnectionConfig(connectionConfig: Connection => Unit): Transactor =
    new Transactor(
      dataSource,
      sqlLogger,
      connectionConfig,
      semaphore
    )

  def connect[A](f: DbCon ?=> A)(using Trace): Task[A] =
    val zio = ZIO.blocking(
      ZIO.acquireReleaseWith(acquireConnection)(releaseConnection)(cn =>
        ZIO.attempt {
          connectionConfig(cn)
          f(using DbCon(cn, sqlLogger))
        }
      )
    )
    semaphore.fold(zio)(_.withPermit(zio))

  def transact[A](f: DbTx ?=> A)(using Trace): Task[A] =
    val zio = ZIO.blocking(
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
              cn.rollback()
              throw t
        }.uninterruptible
      )
    )
    semaphore.fold(zio)(_.withPermit(zio))

  private def acquireConnection(using Trace): Task[Connection] =
    ZIO
      .attempt(dataSource.getConnection())
      .mapError(t => SqlException("Unable to acquire DB Connection", t))

  private def releaseConnection(con: Connection)(using Trace): UIO[Unit] =
    ZIO
      .attempt(con.close())
      .orDieWith(t =>
        SqlException("Unable to close DB Connection, will die", t)
      )
end Transactor

object Transactor:
  private val noOpConnectionConfig: Connection => Unit = _ => ()

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @param maxBlockingThreads
    *   Number of threads in your connection pool. This helps magzio be more
    *   memory efficient by limiting the number of blocking pool threads used.
    *   Not needed if using the ZIO virtual-thread based blocking executor
    * @return
    *   Transactor UIO
    */
  def layer(
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit,
      maxBlockingThreads: Option[Int]
  ): ULayer[Transactor] =
    ZLayer.succeed {
      new Transactor(
        dataSource,
        sqlLogger,
        connectionConfig,
        maxBlockingThreads.map(threads =>
          Semaphore.unsafe.make(threads)(Unsafe)
        )
      )
    }

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @return
    *   Transactor UIO
    */
  def layer(
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): ULayer[Transactor] =
    layer(
      dataSource,
      sqlLogger,
      connectionConfig,
      None
    )

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @return
    *   Transactor UIO
    */
  def layer(dataSource: DataSource, sqlLogger: SqlLogger): ULayer[Transactor] =
    layer(dataSource, sqlLogger, noOpConnectionConfig, None)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @return
    *   Transactor UIO
    */
  def layer(dataSource: DataSource): ULayer[Transactor] =
    layer(dataSource, SqlLogger.Default, noOpConnectionConfig, None)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @return
    *   Transactor UIO
    */
  def layer(
      dataSource: DataSource,
      connectionConfig: Connection => Unit
  ): ULayer[Transactor] =
    layer(dataSource, SqlLogger.Default, connectionConfig, None)

end Transactor
