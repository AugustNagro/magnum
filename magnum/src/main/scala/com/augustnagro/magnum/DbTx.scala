package com.augustnagro.magnum

import java.sql.Connection
import scala.util.Using

/** Represents a transactional [[DbCon]]
  */
class DbTx private[magnum] (connection: Connection, sqlLogger: SqlLogger)
    extends DbCon(connection, sqlLogger)
