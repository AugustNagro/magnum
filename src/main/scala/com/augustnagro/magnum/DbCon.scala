package com.augustnagro.magnum

import java.sql.Connection

/** Simple wrapper around java.sql.Connection. Doesn't have the ability to
  * close() or configure.
  */
class DbCon private[magnum] (val connection: Connection)