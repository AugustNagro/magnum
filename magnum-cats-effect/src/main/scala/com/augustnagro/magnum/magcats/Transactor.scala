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

class Transactor[F[_]: Sync: Monad] private (
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

  def connect[A](f: DbCon ?=> A)(using MonadError[F, Exception]): F[A] =
    val io =
      Resource
        .fromAutoCloseable(acquireConnection)
        .use: cn =>
          Sync[F].delay(connectionConfig(cn)) >>
            Sync[F].interruptible(f(using DbCon(cn, sqlLogger)))

    semaphore.fold(io)(sem => sem.permit.use { _ => io })

  def transact[A](f: DbTx ?=> A)(using MonadError[F, Exception]): F[A] =
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
      ME: MonadError[F, Exception]
  ): F[Connection] =
    val fa = Sync[F].delay(dataSource.getConnection())
    ME.adaptError(fa)(t => SqlException("Unable to acquire DB Connection", t))
end Transactor
