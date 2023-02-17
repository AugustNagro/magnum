package com.augustnagro.magnum

import java.sql.Connection
import scala.util.Using

/** Represents a transactional [[DbCon]]
  */
class DbTx private[magnum] (connection: Connection, manager: Using.Manager)
    extends DbCon(connection, manager)
