package com.augustnagro.magnum.zio

import _root_.zio.{Trace, ZIO}
import com.augustnagro.magnum.*

import javax.sql.DataSource
import scala.util.control.NonFatal

/** Executes a given query on a given DataSource
  *
  * Re-implementation for ZIO of
  * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
  *
  * Usage:
  * {{{
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
  *   transact(dataSource) { tx ?=> repo.insertReturning(creator) }
  * }}}
  */
def transact[A](dataSource: DataSource)(q: DbTx ?=> A)(implicit
    trace: Trace
): ZIO[Any, Throwable, A] =
  transact(Transactor(dataSource))(q)

/** Executes a given transaction on a given Transactor
  *
  * Re-implementation for ZIO of
  * [[com.augustnagro.magnum.transact(transactor: Transactor)]]
  *
  * Usage:
  * {{{
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
