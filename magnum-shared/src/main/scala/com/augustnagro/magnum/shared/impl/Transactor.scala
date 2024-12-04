package com.augustnagro.magnum.shared.impl

import java.sql.Connection
import javax.sql.DataSource

class Transactor(
    val dataSource: DataSource,
    val sqlLogger: SqlLogger,
    val connectionConfig: Connection => Unit
)
