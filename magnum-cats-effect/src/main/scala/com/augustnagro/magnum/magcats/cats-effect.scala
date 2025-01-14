package com.augustnagro.magnum.magcats

import cats.effect.Sync
import cats.effect.Resource
import com.augustnagro.magnum.{DbCon, DbTx, Transactor}

import java.sql.Connection
import javax.sql.DataSource
import scala.util.control.NonFatal

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
def connect[F[_]: Sync, A](dataSource: DataSource)(q: DbCon ?=> A): F[A] =
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
def connect[F[_]: Sync, A](
    transactor: Transactor
)(q: DbCon ?=> A): F[A] =
  Resource
    .fromAutoCloseable(
      Sync[F].interruptible(transactor.dataSource.getConnection())
    )
    .use { cn =>
      Sync[F].interruptible {
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
def transact[F[_]: Sync, A](dataSource: DataSource)(q: DbTx ?=> A): F[A] =
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
def transact[F[_]: Sync, A](
    dataSource: DataSource,
    connectionConfig: Connection => Unit
)(
    q: DbTx ?=> A
): F[A] =
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
def transact[F[_]: Sync, A](
    transactor: Transactor
)(q: DbTx ?=> A): F[A] =
  Resource
    .fromAutoCloseable(
      Sync[F].interruptible(transactor.dataSource.getConnection())
    )
    .use { cn =>
      Sync[F].blocking {
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
