package com.augustnagro.magnum.shared.builders

import com.augustnagro.magnum.logging.SqlLogger

import java.sql.Connection
import javax.sql.DataSource

/** Transactor lets you customize a transaction or connection's behavior. It is
  * a parameter to the [[transact]] and [[connect]] methods.
  */
class Transactor(
    /** Datasource to be used */
    val dataSource: DataSource,
    /** Logging configuration */
    val sqlLogger: SqlLogger = SqlLogger.Default,
    /** Customize the underlying JDBC Connections */
    val connectionConfig: Connection => Unit = con => ()
)


