package com.augustnagro.magnum.errors

class SqlException private[magnum] (message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
