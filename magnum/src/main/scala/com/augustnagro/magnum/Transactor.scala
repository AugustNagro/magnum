package com.augustnagro.magnum

import java.sql.Connection
import javax.sql.DataSource
import scala.util.Using

class Transactor private (
    dataSource: DataSource,
    sqlLogger: SqlLogger = SqlLogger.Default,
    connectionConfig: Connection => Unit = con => ()
):
  def withSqlLogger(sqlLogger: SqlLogger): Transactor =
    new Transactor(dataSource, sqlLogger, connectionConfig)

  def withConnectionConfig(connectionConfig: Connection => Unit): Transactor =
    new Transactor(dataSource, sqlLogger, connectionConfig)

  def connect[T](f: DbCon ?=> T): T =
    Using.resource(dataSource.getConnection): con =>
      connectionConfig(con)
      f(using DbCon(con, sqlLogger))

  def transact[T](f: DbTx ?=> T): T =
    Using.resource(dataSource.getConnection): con =>
      connectionConfig(con)
      con.setAutoCommit(false)
      try
        val res = f(using DbTx(con, sqlLogger))
        con.commit()
        res
      catch
        case t =>
          try con.rollback()
          catch { case t2 => t.addSuppressed(t2) }
          throw t
end Transactor

object Transactor:

  def apply(
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): Transactor =
    new Transactor(dataSource, sqlLogger, connectionConfig)

  def apply(dataSource: DataSource, sqlLogger: SqlLogger): Transactor =
    new Transactor(dataSource, sqlLogger, _ => ())

  def apply(
      dataSource: DataSource,
      connectionConfig: Connection => Unit
  ): Transactor =
    new Transactor(dataSource, SqlLogger.Default, connectionConfig)

  def apply(dataSource: DataSource): Transactor =
    new Transactor(dataSource, SqlLogger.Default, _ => ())

end Transactor
