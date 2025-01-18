package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{
  DbCon,
  DbTx,
  SqlException,
  SqlLogger,
  TransactorOps
}
import zio.{Semaphore, Task, Trace, UIO, ULayer, ZIO, ZLayer}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

final case class Transactor(
    private val dataSource: DataSource,
    private val sqlLogger: SqlLogger,
    private val connectionConfig: Connection => Unit,
    private val semaphore: Option[Semaphore]
) extends TransactorOps[Task]:

  def withSqlLogger(sqlLogger: SqlLogger): Transactor =
    copy(sqlLogger = sqlLogger)

  def withConnectionConfig(connectionConfig: Connection => Unit) =
    copy(connectionConfig = connectionConfig)

  def connect[A](f: DbCon ?=> A): Task[A] =
    val zio = ZIO.blocking(
      ZIO.acquireReleaseWith(acquireConnection)(releaseConnection)(cn =>
        ZIO.attempt {
          connectionConfig(cn)
          f(using DbCon(cn, sqlLogger))
        }
      )
    )
    semaphore.fold(zio)(_.withPermit(zio))

  def transact[A](f: DbTx ?=> A): Task[A] =
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
  private val defaultLogger = ZLayer.succeed(SqlLogger.Default)
  private val defaultConnectionConfig = ZLayer.succeed(noOpConnectionConfig)
  private val defaultSemaphore = ZLayer.succeed(None)

  def layer = ZLayer.derive[Transactor]

  /** Configures a transactor layer with the given parameters.
    *
    * @param sqlLogger
    * @param connectionConfig
    * @param maxBlockingThreads
    * @return
    *   The transactor layer
    */
  def configured(
      sqlLogger: SqlLogger = SqlLogger.Default,
      connectionConfig: Connection => Unit = noOpConnectionConfig,
      maxBlockingThreads: Int = -1
  ) =
    val semaphore = ZLayer {
      if maxBlockingThreads < 1 then ZIO.succeed(None)
      else Semaphore.make(maxBlockingThreads).map(Some(_))
    }
    defaultLogger ++ defaultConnectionConfig ++ semaphore >>> layer

  /** The default transactor layer with all dependencies satisfied except for
    * the DataSource.
    */
  val default: ZLayer[DataSource, Nothing, Transactor] =
    defaultLogger ++ defaultConnectionConfig ++ defaultSemaphore >>> layer

end Transactor
