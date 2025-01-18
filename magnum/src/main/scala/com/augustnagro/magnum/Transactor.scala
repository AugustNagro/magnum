package com.augustnagro.magnum

import java.sql.Connection
import javax.sql.DataSource
import scala.util.Using

trait TransactorOps[T[_]]:
  def connect[A](f: DbCon ?=> A): T[A]
  def transact[A](f: DbTx ?=> A): T[A]

type Plain[A] = A
object Plain:
  given makePlain[A]: Conversion[A, Plain[A]] = a => a: Plain[A]

final case class Transactor(
    private val dataSource: DataSource,
    private val sqlLogger: SqlLogger = SqlLogger.Default,
    private val connectionConfig: Connection => Unit = con => ()
) extends TransactorOps[Plain]:

  def withSqlLogger(sqlLogger: SqlLogger) = copy(sqlLogger = sqlLogger)

  def withConnectionConfig(connectionConfig: Connection => Unit) =
    copy(connectionConfig = connectionConfig)

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
          con.rollback()
          throw t
end Transactor
