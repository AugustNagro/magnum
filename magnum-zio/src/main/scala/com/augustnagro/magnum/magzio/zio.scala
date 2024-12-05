package com.augustnagro.magnum.magzio

import zio.{Trace, ZIO}
import com.augustnagro.magnum.*

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

/** Executes a given query on a given DataSource
  *
  * Re-implementation for ZIO of
  * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magzio.*
  *
  *   connect(datasource) { cn ?=> repo.findById(id) }
  * }}}
  */
def connect[A](dataSource: DataSource)(q: DbCon ?=> A)(implicit
    trace: Trace
): ZIO[Any, Throwable, A] =
  connect(Transactor(dataSource))(q)

/** Executes a given query on a given Transactor
  *
  * Re-implementation for ZIO of
  * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magzio.*
  *
  *   connect(transactor) { cn ?=> repo.findById(id) }
  * }}}
  */
def connect[A](
    transactor: Transactor
)(q: DbCon ?=> A)(implicit trace: Trace): ZIO[Any, Throwable, A] =
  ZIO.blocking {
    ZIO.acquireReleaseWith(
      acquire = ZIO.attempt(transactor.dataSource.getConnection())
    )(release =
      conn => if (conn ne null) ZIO.attempt(conn.close()).orDie else ZIO.unit
    ) { cn =>
      ZIO.attempt {
        transactor.connectionConfig(cn)
        q(using DbCon(cn, transactor.sqlLogger))
      }
    }
  }

/** Executes a given transaction on a given DataSource
  *
  * Re-implementation for ZIO of
  * [[com.augustnagro.magnum.transact(transactor: Transactor)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magzio.*
  *
  *   transact(dataSource) { tx ?=> repo.insertReturning(creator) }
  * }}}
  */
def transact[A](dataSource: DataSource)(q: DbTx ?=> A)(implicit
    trace: Trace
): ZIO[Any, Throwable, A] =
  transact(Transactor(dataSource))(q)

/** Executes a given transaction on a given DataSource
  *
  * Re-implementation for ZIO of
  * [[com.augustnagro.magnum.transact(transactor: Transactor, connectionConfig: Connection => Unit)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magzio.*
  *
  *   transact(dataSource, ...) { tx ?=> repo.insertReturning(creator) }
  * }}}
  */
def transact[A](dataSource: DataSource, connectionConfig: Connection => Unit)(
    q: DbTx ?=> A
)(implicit trace: Trace): ZIO[Any, Throwable, A] =
  val transactor =
    Transactor(dataSource = dataSource, connectionConfig = connectionConfig)
  transact(transactor)(q)

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
def transact[A](
    transactor: Transactor
)(q: DbTx ?=> A)(implicit trace: Trace): ZIO[Any, Throwable, A] =
  ZIO.blocking {
    ZIO.acquireReleaseWith(
      acquire = ZIO.attempt(transactor.dataSource.getConnection())
    )(release =
      conn => if (conn ne null) ZIO.attempt(conn.close()).orDie else ZIO.unit
    ) { cn =>
      ZIO.uninterruptible {
        ZIO.attempt {
          transactor.connectionConfig(cn)
          cn.setAutoCommit(false)
          try {
            val res = q(using DbTx(cn, transactor.sqlLogger))
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
