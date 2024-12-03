package com.augustnagro.magnum.builders

import com.augustnagro.magnum.builders.DbCon
import com.augustnagro.magnum.logging.SqlLogger

import java.sql.Connection
import scala.util.Using

/** Represents a transactional [[DbCon]]
  */
class DbTx private[magnum] (connection: Connection, sqlLogger: SqlLogger)
    extends DbCon(connection, sqlLogger)
