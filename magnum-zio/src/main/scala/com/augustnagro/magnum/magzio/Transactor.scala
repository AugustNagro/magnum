package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{DbCon, DbTx, SqlLogger}
import zio.{IO, Semaphore, Task, Trace, UIO, ZIO}

import java.sql.Connection
import javax.sql.DataSource
import scala.reflect.ClassTag
import scala.util.control.{ControlThrowable, NonFatal}

/** Transactor lets you customize a transaction or connection's behavior. It is
  * a parameter to the [[transact]] and [[connect]] methods.
  */
class Transactor private (
    /** Datasource to be used */
    private val dataSource: DataSource,
    /** Logging configuration */
    private val sqlLogger: SqlLogger,
    /** Customize the underlying JDBC Connections */
    private val connectionConfig: Connection => Unit,
    /** Number of threads in your connection pool. This helps magzio be more
      * memory efficient by limiting the number of blocking pool threads used.
      * Not needed if using the ZIO virtual-thread based blocking executor
      */
    private val semaphore: Option[Semaphore]
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

  /** Executes a given query on a given Transactor, returning UIO.
    *
    * Re-implementation for ZIO of
    * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
    *
    * Usage:
    * {{{
    *   import com.augustnagro.magnum.magzio.*
    *
    *   connectUIO(transactor):
    *      repo.findById(id)
    * }}}
    */
  def connectUIO[A](q: DbCon ?=> A)(using Trace): UIO[A] = connectTask(q).orDie

  /** Executes a given query on a given Transactor, returning IO.
    *
    * Re-implementation for ZIO of
    * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
    *
    * Usage:
    * {{{
    *   import com.augustnagro.magnum.magzio.*
    *
    *   connectIO(transactor):
    *      repo.findById(id)
    * }}}
    */
  def connectIO[E: ClassTag, A](q: DbCon ?=> A)(using Trace): IO[E, A] =
    connectTask(q).catchAll:
      case break: Break[?]  => ZIO.fail(break.value.asInstanceOf[E])
      case error: E         => ZIO.fail(error)
      case NonFatal(defect) => ZIO.die(defect)

  private def connectTask[A](q: DbCon ?=> A)(using Trace): Task[A] =
    val task = ZIO.blocking(
      ZIO.acquireReleaseWith(
        acquire = ZIO.attempt(dataSource.getConnection())
      )(release = conn => ZIO.attempt(conn.close()).orDie): cn =>
        ZIO.attempt:
          connectionConfig(cn)
          q(using DbCon(cn, sqlLogger))
    )
    semaphore match
      case Some(sem) => sem.withPermit(task)
      case None      => task

  /** Executes a given transaction on a given Transactor
    *
    * Re-implementation for ZIO of
    * [[com.augustnagro.magnum.transact(transactor: Transactor)]]
    *
    * Usage:
    * {{{
    *   import com.augustnagro.magnum.magzio.*
    *
    *   transact(transactor) { tx ?=> repo.insertReturning(creator) }
    * }}}
    */
  def transactUIO[A](q: DbTx ?=> A)(using Trace): UIO[A] =
    transactTask(q).orDie

  def transactIO[E: ClassTag, A](q: DbTx ?=> A)(using Trace): IO[E, A] =
    transactTask(q).catchAll:
      case break: Break[?]  => ZIO.fail(break.value.asInstanceOf[E])
      case error: E         => ZIO.fail(error)
      case NonFatal(defect) => ZIO.die(defect)

  private def transactTask[A](q: DbTx ?=> A)(using Trace): Task[A] =
    val task = ZIO.blocking {
      ZIO.acquireReleaseWith(
        acquire = ZIO.attempt(dataSource.getConnection())
      )(release =
        conn => if (conn ne null) ZIO.attempt(conn.close()).orDie else ZIO.unit
      ) { cn =>
        ZIO.uninterruptible {
          ZIO.attempt {
            connectionConfig(cn)
            cn.setAutoCommit(false)
            try {
              val res = q(using DbTx(cn, sqlLogger))
              cn.commit()
              res
            } catch {
              case NonFatal(t) =>
                cn.rollback()
                throw t
            }
          }
        }
      }
    }
    semaphore match
      case Some(sem) => sem.withPermit(task)
      case None      => task
  end transactTask
end Transactor

object Transactor:
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

  def apply(dataSource: DataSource, sqlLogger: SqlLogger): UIO[Transactor] =
    apply(
      dataSource,
      sqlLogger,
      _ => (),
      None
    )

  def apply(dataSource: DataSource): UIO[Transactor] =
    apply(
      dataSource,
      SqlLogger.Default,
      _ => (),
      None
    )

  def apply(
      dataSource: DataSource,
      connectionConfig: Connection => Unit
  ): UIO[Transactor] =
    apply(
      dataSource,
      SqlLogger.Default,
      connectionConfig,
      None
    )
end Transactor
