package com.augustnagro.magnum.magcats

import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal
import cats.effect.std.Semaphore
import cats.effect.kernel.Sync
import cats.effect.kernel.Resource
import cats.MonadError
import cats.Monad
import cats.syntax.all.*
import cats.effect.kernel.Concurrent
import cats.effect.kernel.Async

class Transactor[F[_]: Async] private (
    dataSource: DataSource,
    sqlLogger: SqlLogger,
    connectionConfig: Connection => Unit,
    semaphore: Option[Semaphore[F]]
):
  def withSqlLogger(sqlLogger: SqlLogger): Transactor[F] =
    new Transactor(
      dataSource,
      sqlLogger,
      connectionConfig,
      semaphore
    )

  def withConnectionConfig(
      connectionConfig: Connection => Unit
  ): Transactor[F] =
    new Transactor(
      dataSource,
      sqlLogger,
      connectionConfig,
      semaphore
    )

  def connect[A](f: DbCon ?=> A)(using MonadError[F, Throwable]): F[A] =
    val io =
      Resource
        .fromAutoCloseable(acquireConnection)
        .use: cn =>
          Sync[F].delay(connectionConfig(cn)) >>
            Sync[F].interruptible(f(using DbCon(cn, sqlLogger)))

    semaphore.fold(io)(sem => sem.permit.use { _ => io })

  def transact[A](f: DbTx ?=> A)(using MonadError[F, Throwable]): F[A] =
    val io =
      Resource
        .fromAutoCloseable(acquireConnection)
        .use: cn =>
          Sync[F].delay(connectionConfig(cn)) >>
            Sync[F].delay(cn.setAutoCommit(false)) >>
            Sync[F].blocking(
              try
                val res = f(using DbTx(cn, sqlLogger))
                cn.commit()
                res
              catch
                case NonFatal(t) =>
                  cn.rollback()
                  throw t
            )

    semaphore.fold(io)(sem => sem.permit.use { _ => io })

  private def acquireConnection(using
      ME: MonadError[F, Throwable]
  ): F[Connection] =
    val fa = Async[F].delay(dataSource.getConnection())
    ME.adaptError(fa)(t => SqlException("Unable to acquire DB Connection", t))
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
    *   Number of threads in your connection pool. This helps magcats be more
    *   memory efficient by limiting the number of blocking pool threads used.
    *   Not needed if using a virtual-thread based blocking executor (e.g. via
    *   evalOn)
    * @return
    *   F[Transactor[F]]
    */
  // TODO: Figure out if the maxBlockingThreads description is actually true
  def apply[F[_]: Async](
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit,
      maxBlockingThreads: Option[Int]
  ): F[Transactor[F]] =
    // Figure out what to throw here
    maxBlockingThreads
      .map(threads => Semaphore(threads))
      .fold {
        Async[F].delay:
          new Transactor[F](
            dataSource,
            sqlLogger,
            connectionConfig,
            None
          )
      } { semaphore =>
        semaphore.map: sem =>
          new Transactor(dataSource, sqlLogger, connectionConfig, Some(sem))
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
    *   F[Transactor[F]]
    */
  def apply[F[_]: Async](
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): F[Transactor[F]] =
    apply(dataSource, sqlLogger, connectionConfig, None)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @return
    *   F[Transactor[F]]
    */
  def apply[F[_]: Async](
      dataSource: DataSource,
      sqlLogger: SqlLogger
  ): F[Transactor[F]] =
    apply(dataSource, sqlLogger, noOpConnectionConfig, None)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @return
    *   F[Transactor[F]]
    */
  def apply[F[_]: Async](
      dataSource: DataSource
  ): F[Transactor[F]] =
    apply(dataSource, SqlLogger.Default, noOpConnectionConfig, None)
end Transactor
