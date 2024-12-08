package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}
import zio.{IO, Semaphore, Task, Trace, UIO, URIO, ZIO}

import java.sql.Connection
import javax.sql.DataSource
import scala.reflect.ClassTag
import scala.util.control.{ControlThrowable, NonFatal}

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
    ZIO.blocking(
      ZIO.acquireReleaseWith(acquireConnection)(releaseConnection)(cn =>
        ZIO.attempt {
          connectionConfig(cn)
          f(using DbCon(cn, sqlLogger))
        }.uninterruptible
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
              cn.rollback()
              throw t
        }.uninterruptible
      )
    )

  private def acquireConnection(using Trace): Task[Connection] =
    ZIO
      .attempt(dataSource.getConnection())
      .mapError(t => SqlException("Unable to acquire DB Connection", t))

  private def releaseConnection[R](con: Connection)(using Trace): URIO[R, Any] =
    ZIO
      .attempt(con.close())
      .orDieWith(t =>
        SqlException("Unable to close DB Connection, will die", t)
      )
end Transactor

object Transactor:

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
  def apply(
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit,
      maxBlockingThreads: Option[Int]
  ): UIO[Transactor] =
    ZIO
      .fromOption(maxBlockingThreads)
      .flatMap(threads => Semaphore.make(threads))
      .unsome
      .map(semaphoreOpt =>
        new Transactor(
          dataSource,
          sqlLogger,
          connectionConfig,
          semaphoreOpt
        )
      )

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
  def apply(
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): UIO[Transactor] =
    apply(
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
  def apply(dataSource: DataSource, sqlLogger: SqlLogger): UIO[Transactor] =
    apply(dataSource, sqlLogger, _ => (), None)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @return
    *   Transactor UIO
    */
  def apply(dataSource: DataSource): UIO[Transactor] =
    apply(dataSource, SqlLogger.Default, _ => (), None)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @return
    *   Transactor UIO
    */
  def apply(
      dataSource: DataSource,
      connectionConfig: Connection => Unit
  ): UIO[Transactor] =
    apply(dataSource, SqlLogger.Default, connectionConfig, None)

end Transactor
