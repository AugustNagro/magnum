package com.augustnagro.magnum.magzio.builders

import com.augustnagro.magnum.magzio.logging.SqlLogger
import com.augustnagro.magnum.magzio.builders.{MUIO, MIO}
import zio.*

import java.sql.Connection
import javax.sql.DataSource

class Transactor private (
    /** Datasource to be used */
    val dataSource: DataSource,
    /** Logging configuration */
    val sqlLogger: SqlLogger,
    /** Customize the underlying JDBC Connections */
    val connectionConfig: Connection => MUIO[Unit]
):

  def withDataSource(dataSource: DataSource): Transactor =
    new Transactor(dataSource, sqlLogger, connectionConfig)

  def withSqlLogger(sqlLogger: SqlLogger): Transactor =
    new Transactor(dataSource, sqlLogger, connectionConfig)

  def withConnectionConfig(connectionConfig: Connection => Unit): Transactor =
    new Transactor(dataSource, sqlLogger, connectionConfig)

  // todo error type
  // todo max parallelism
  def transact[E, A](mio: MIO[E, A]): IO[E, A] =
    ZIO.blocking(
      ZIO.acquireReleaseWith(
        acquire = ZIO.attempt(dataSource.getConnection)
      )(release = con => ZIO.attempt(con.close()).orDie)(con =>
        connectionConfig(con)
          .flatMap(_ => MIO.attempt(con.setAutoCommit(false)))
          .flatMap(_ => mio)
      )
    )
end Transactor

object Transactor:
  def apply(
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): Transactor = new Transactor(dataSource, sqlLogger, connectionConfig)

  def apply(dataSource: DataSource, sqlLogger: SqlLogger): Transactor =
    apply(dataSource, sqlLogger, con => ())

  def apply(dataSource: DataSource): Transactor =
    apply(dataSource, SqlLogger.Default)
