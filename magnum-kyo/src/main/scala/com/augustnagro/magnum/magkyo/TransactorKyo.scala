package com.augustnagro.magnum.magkyo

import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}
import kyo.*

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

class TransactorKyo private (
    dataSource: DataSource,
    sqlLogger: SqlLogger,
    connectionConfig: Connection => Unit,
    semaphore: Maybe[Meter]
):
  def withSqlLogger(sqlLogger: SqlLogger): TransactorKyo =
    new TransactorKyo(
      dataSource,
      sqlLogger,
      connectionConfig,
      semaphore
    )

  def withConnectionConfig(
      connectionConfig: Connection => Unit
  ): TransactorKyo =
    new TransactorKyo(
      dataSource,
      sqlLogger,
      connectionConfig,
      semaphore
    )

  def connect[A](f: DbCon ?=> A): A < (Abort[Throwable] & Async) =
    val res =
      Resource
        .acquireRelease(acquireConnection)(releaseConnection)
        .map { lo =>
          IO {
            connectionConfig(lo)
            f(using DbCon(lo, sqlLogger))
          }
        }
    val effect = Resource.run(res)
    semaphore.fold(effect)(_.run(effect))

  def transact[A](f: DbTx ?=> A): A < (Abort[Throwable] & Async) =
    val res =
      Resource
        .acquireRelease(acquireConnection)(releaseConnection)
        .map { lo =>
          Async.mask {
            connectionConfig(lo)
            lo.setAutoCommit(false)
            Abort
              .catching[Throwable](
                f(using DbTx(lo, sqlLogger))
              )
              .foldAbort(
                out =>
                  IO {
                    lo.commit()
                    out
                  },
                error =>
                  IO {
                    lo.rollback()
                    Abort.fail(error)
                  }
              )
          }
        }
    val effect = Resource.run(res)
    semaphore.fold(effect)(_.run(effect))
  end transact

  private val acquireConnection: Connection < (IO & Abort[SqlException]) =
    IO(
      Abort
        .catching[Throwable](dataSource.getConnection())
        .mapAbort[SqlException, Any](t =>
          SqlException("Unable to acquire DB Connection", t)
        )
    )

  private def releaseConnection(con: Connection) =
    if con eq null then ()
    else
      IO(
        Abort
          .catching[Throwable](con.close())
          .mapAbort[SqlException, Any](t =>
            SqlException("Unable to close DB Connection", t)
          )
          .orPanic
      )
end TransactorKyo

object TransactorKyo:
  private val noOpConnectionConfig: Connection => Unit = _ => ()

  /** Construct a TransactorKyo
    *
    * @param sqlLogger
    *   Logging configuration
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    * @param maxBlockingThreads
    *   Number of threads in your connection pool. This helps the library be
    *   more memory efficient by limiting the number of blocking pool threads
    *   used.
    */
  def layer(
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit,
      maxBlockingThreads: Maybe[Int]
  ): Layer[TransactorKyo, Env[DataSource] & IO] = Layer {
    for
      dataSource <- Env.get[DataSource]
      semaphore <- maxBlockingThreads.fold(IO(Maybe.empty[Meter]))(max =>
        Meter.initSemaphore(max).map(Maybe(_))
      )
      transactor <- IO(
        new TransactorKyo(
          dataSource,
          sqlLogger,
          connectionConfig,
          semaphore
        )
      )
    yield transactor
  }

  /** Construct a TransactorKyo
    *
    * @param sqlLogger
    *   Logging configuration
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    */
  def layer(
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): Layer[TransactorKyo, Env[DataSource] & IO] =
    layer(
      sqlLogger = sqlLogger,
      connectionConfig = connectionConfig,
      maxBlockingThreads = Maybe.empty
    )

  /** Construct a TransactorKyo
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    */
  def layer(sqlLogger: SqlLogger): Layer[TransactorKyo, Env[DataSource] & IO] =
    layer(
      sqlLogger = sqlLogger,
      connectionConfig = noOpConnectionConfig,
      maxBlockingThreads = Maybe.Absent
    )

  /** Construct a TransactorKyo */
  def layer: Layer[TransactorKyo, Env[DataSource] & IO] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = noOpConnectionConfig,
      maxBlockingThreads = Maybe.Absent
    )

  /** Construct a TransactorZIO
    *
    * @param connectionConfig
    *   Customize the underlying JDBC Connections
    */
  def layer(
      connectionConfig: Connection => Unit
  ): Layer[TransactorKyo, Env[DataSource] & IO] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = connectionConfig,
      maxBlockingThreads = Maybe.Absent
    )

  /** @param maxBlockingThreads
    *   Number of threads in your connection pool. This helps magzio be more
    *   memory efficient by limiting the number of blocking pool threads used.
    *   Not needed if using the ZIO virtual-thread based blocking executor
    */
  def layer(
      maxBlockingThreads: Int
  ): Layer[TransactorKyo, Env[DataSource] & IO] =
    layer(
      sqlLogger = SqlLogger.Default,
      connectionConfig = noOpConnectionConfig,
      maxBlockingThreads = Maybe(maxBlockingThreads)
    )

end TransactorKyo
