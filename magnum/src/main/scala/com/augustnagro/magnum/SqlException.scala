package com.augustnagro.magnum

class SqlException private[magnum] (message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
