package com.augustnagro.magnum.builders

import com.augustnagro.magnum.logging.SqlLogger

import java.sql.Connection
import javax.sql.DataSource

/** Transactor lets you customize a transaction or connection's behavior. It is
  * a parameter to the [[transact]] and [[connect]] methods.
  */
case class Transactor(
    /** Datasource to be used */
    dataSource: DataSource,
    /** Logging configuration */
    sqlLogger: SqlLogger = SqlLogger.Default,
    /** Customize the underlying JDBC Connections */
    connectionConfig: Connection => Unit = con => ()
)
