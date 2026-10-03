package com.augustnagro.magnum.magcats

import cats.effect.kernel.Sync
import com.augustnagro.magnum.{DbCon, DbTx, SqlException, SqlLogger}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.Using
import scala.util.Using.Releasable
import scala.util.control.NonFatal

final class TransactorCats[F[_]] private (
    dataSource: DataSource,
    sqlLogger: SqlLogger,
    connectionConfig: Connection => Unit
)(using F: Sync[F]):

  private given Releasable[Connection] = con => releaseConnection(con)

  def withSqlLogger(logger: SqlLogger): TransactorCats[F] =
    new TransactorCats(dataSource, logger, connectionConfig)

  def withConnectionConfig(
      config: Connection => Unit
  ): TransactorCats[F] =
    new TransactorCats(dataSource, sqlLogger, config)

  def connect[A](f: DbCon ?=> A): F[A] =
    F.blocking {
      Using.resource(acquireConnection()) { cn =>
        connectionConfig(cn)
        f(using DbCon(cn, sqlLogger))
      }
    }

  def transact[A](f: DbTx ?=> A): F[A] =
    F.blocking {
      Using.resource(acquireConnection()) { cn =>
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
      }
    }

  private def acquireConnection(): Connection =
    try dataSource.getConnection()
    catch
      case NonFatal(t) =>
        throw SqlException("Unable to acquire DB Connection", t)

  private def releaseConnection(cn: Connection): Unit =
    if cn ne null then
      try cn.close()
      catch
        case NonFatal(t) =>
          throw SqlException("Unable to close DB Connection", t)

end TransactorCats

object TransactorCats:
  private val noOpConnectionConfig: Connection => Unit = _ => ()

  def apply[F[_]: Sync](
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): TransactorCats[F] =
    new TransactorCats(dataSource, sqlLogger, connectionConfig)

  def apply[F[_]: Sync](
      dataSource: DataSource,
      sqlLogger: SqlLogger
  ): TransactorCats[F] =
    apply(dataSource, sqlLogger, noOpConnectionConfig)

  def apply[F[_]: Sync](
      dataSource: DataSource,
      connectionConfig: Connection => Unit
  ): TransactorCats[F] =
    apply(dataSource, SqlLogger.Default, connectionConfig)

  def apply[F[_]: Sync](dataSource: DataSource): TransactorCats[F] =
    apply(dataSource, SqlLogger.Default, noOpConnectionConfig)

end TransactorCats
