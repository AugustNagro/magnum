package com.augustnagro.magnum.magcats

import cats.*
import cats.effect.kernel.*
import cats.effect.kernel.Outcome.*
import cats.effect.std.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.augustnagro.magnum.*

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

class Transactor[F[_]: Sync] private (
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

  def connect[A](f: DbCon ?=> A): F[A] =
    val io =
      Resource
        .make(acquireConnection)(releaseConnection)
        .use: cn =>
          Sync[F].delay(connectionConfig(cn)) >>
            Sync[F].interruptible(f(using DbCon(cn, sqlLogger)))

    semaphore.fold(io)(sem => sem.permit.use { _ => io })

  def transact[A](f: DbTx ?=> A): F[A] =
    val io =
      Resource
        .make(acquireConnection)(releaseConnection)
        .use: cn =>
          Sync[F].delay(connectionConfig(cn)) >>
            Sync[F].delay(cn.setAutoCommit(false)) >>
            Sync[F]
              .interruptible(f(using DbTx(cn, sqlLogger)))
              .guaranteeCase {
                case Succeeded(_) => Sync[F].blocking(cn.commit())
                case Errored(_) | Canceled() =>
                  Sync[F].blocking(cn.rollback())
              }

    semaphore.fold(io)(sem => sem.permit.use { _ => io })

  private def acquireConnection: F[Connection] =
    Sync[F]
      .delay(dataSource.getConnection())
      .adaptError(t => SqlException("Unable to acquire DB Connection", t))

  private def releaseConnection(conn: Connection): F[Unit] =
    if conn eq null then Sync[F].unit
    else
      Sync[F]
        .delay(conn.close())
        .adaptError(t => SqlException("Unable to close DB connection", t))
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
  def apply[F[_]: Async](
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit,
      maxBlockingThreads: Int
  ): F[Transactor[F]] =
    Semaphore[F](maxBlockingThreads).map: semaphore =>
      new Transactor(
        dataSource,
        sqlLogger,
        connectionConfig,
        Some(semaphore)
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
    *   F[Transactor[F]]
    */
  def apply[F[_]: Sync](
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): F[Transactor[F]] =
    Sync[F].delay(new Transactor(dataSource, sqlLogger, connectionConfig, None))

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @return
    *   F[Transactor[F]]
    */
  def apply[F[_]: Sync](
      dataSource: DataSource,
      sqlLogger: SqlLogger
  ): F[Transactor[F]] =
    apply(dataSource, sqlLogger, noOpConnectionConfig)

  /** Construct a Transactor
    *
    * @param dataSource
    *   Datasource to be used
    * @param sqlLogger
    *   Logging configuration
    * @return
    *   F[Transactor[F]]
    */
  def apply[F[_]: Sync](
      dataSource: DataSource
  ): F[Transactor[F]] =
    apply(dataSource, SqlLogger.Default, noOpConnectionConfig)
end Transactor
