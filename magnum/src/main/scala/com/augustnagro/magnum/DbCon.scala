package com.augustnagro.magnum

import java.sql.{CallableStatement, Connection, PreparedStatement, Statement}
import scala.util.Using

/** Simple wrapper around java.sql.Connection. Doesn't have the ability to
  * close() or configure.
  */
class DbCon private[magnum] (val connection: Connection, val manager: Using.Manager)