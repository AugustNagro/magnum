package com.augustnagro.magnum

import java.sql.Connection

/** Simple wrapper around java.sql.Connection. See
  * `com.augustnagro.magnum.connect` and `transact`
  */
class DbCon private[magnum] (
    val connection: Connection,
    val sqlLogger: SqlLogger
)
