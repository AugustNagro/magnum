package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}
import zio.{Semaphore, Task, Trace, UIO, URLayer, ZIO, ZLayer}

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
    if con eq null then ZIO.unit
    else
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
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit,
      maxBlockingThreads: Option[Int]
  ): URLayer[DataSource, Transactor] =
    ZLayer.fromZIO {
      for {
        dataSource <- ZIO.service[DataSource]
        transactor <- ZIO
          .fromOption(maxBlockingThreads)
          .flatMap(threads => Semaphore.make(threads))
          .unsome
          .map(semaphoreOpt =>
            new Transactor(
              dataSource = dataSource,
              sqlLogger = sqlLogger,
              connectionConfig = connectionConfig,
              semaphore = semaphoreOpt
            )
          )
      } yield transactor
    }

  /** Construct a Transactor
    *
    * @param sqlLogger
    *   Logging configuration
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @return
    *   Transactor UIO
    */
  def layer(
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): URLayer[DataSource, Transactor] =
    layer(
      sqlLogger = sqlLogger,
      connectionConfig = connectionConfig,
      maxBlockingThreads = None
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
  def layer(sqlLogger: SqlLogger): URLayer[DataSource, Transactor] =
    layer(
      sqlLogger = sqlLogger,
      connectionConfig = noOpConnectionConfig,
      maxBlockingThreads = None
    )

  /** Construct a Transactor
    *
    * @return
    *   Transactor UIO
    */
  def layer: URLayer[DataSource, Transactor] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = noOpConnectionConfig,
      maxBlockingThreads = None
    )

  /** Construct a Transactor
    *
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @return
    *   Transactor UIO
    */
  def layer(
      connectionConfig: Connection => Unit
  ): URLayer[DataSource, Transactor] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = connectionConfig,
      maxBlockingThreads = None
    )

  /** @param maxBlockingThreads
    *   Number of threads in your connection pool. This helps magzio be more
    *   memory efficient by limiting the number of blocking pool threads used.
    *   Not needed if using the ZIO virtual-thread based blocking executor
    * @return
    */
  def layer(maxBlockingThreads: Int): URLayer[DataSource, Transactor] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = noOpConnectionConfig,
      maxBlockingThreads = Some(maxBlockingThreads)
    )

end Transactor
