package com.augustnagro.magnum.magcats

import javax.sql.DataSource
import com.augustnagro.magnum.DbCon
import cats.effect.IO
import com.augustnagro.magnum.Transactor
import cats.effect.kernel.Resource
import java.sql.Connection
import com.augustnagro.magnum.DbTx
import cats.effect.unsafe.IORuntime
import scala.util.control.NonFatal
import natchez.Trace

/** Executes a given query on a given DataSource
  *
  * Re-implementation for IO of
  * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magcats.*
  *
  *   connect(datasource) { cn ?=> repo.findById(id) }
  * }}}
  */
def connect[A](dataSource: DataSource)(q: DbCon ?=> A)(using
    trace: Trace[IO]
): IO[A] =
  connect(Transactor(dataSource))(q)

/** Executes a given query on a given Transactor
  *
  * Re-implementation for IO of
  * [[com.augustnagro.magnum.connect(dataSource: DataSource)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magcats.*
  *
  *   connect(transactor) { cn ?=> repo.findById(id) }
  * }}}
  */
def connect[A](
    transactor: Transactor
)(q: DbCon ?=> A)(using trace: Trace[IO]): IO[A] =
  Resource
    .fromAutoCloseable(IO.interruptible(transactor.dataSource.getConnection()))
    .use { cn =>
      IO.interruptible {
        transactor.connectionConfig(cn)
        q(using DbCon(cn, transactor.sqlLogger))
      }
    }

/** Executes a given transaction on a given DataSource
  *
  * Re-implementation for IO of
  * [[com.augustnagro.magnum.transact(transactor: Transactor)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magcats.*
  *
  *   transact(dataSource) { tx ?=> repo.insertReturning(creator) }
  * }}}
  */
def transact[A](dataSource: DataSource)(q: DbTx ?=> A)(using
    trace: Trace[IO]
): IO[A] =
  transact(Transactor(dataSource))(q)

/** Executes a given transaction on a given DataSource
  *
  * Re-implementation for IO of
  * [[com.augustnagro.magnum.transact(transactor: Transactor, connectionConfig: Connection => Unit)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magcats.*
  *
  *   transact(dataSource, ...) { tx ?=> repo.insertReturning(creator) }
  * }}}
  */
def transact[A](dataSource: DataSource, connectionConfig: Connection => Unit)(
    q: DbTx ?=> A
)(using trace: Trace[IO]): IO[A] =
  val transactor =
    Transactor(dataSource = dataSource, connectionConfig = connectionConfig)
  transact(transactor)(q)

/** Executes a given transaction on a given Transactor
  *
  * Re-implementation for IO of
  * [[com.augustnagro.magnum.transact(transactor: Transactor)]]
  *
  * Usage:
  * {{{
  *   import com.augustnagro.magnum.magcats.*
  *
  *   transact(transactor) { tx ?=> repo.insertReturning(creator) }
  * }}}
  */
def transact[A](
    transactor: Transactor
)(q: DbTx ?=> A)(using trace: Trace[IO]): IO[A] =
  Resource
    .fromAutoCloseable(IO.interruptible(transactor.dataSource.getConnection()))
    .use { cn =>
      IO.blocking {
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
