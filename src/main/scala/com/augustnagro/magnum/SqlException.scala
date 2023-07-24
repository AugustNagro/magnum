package com.augustnagro.magnum

class SqlException(query: String, params: Any, t: Throwable = null)
    extends RuntimeException(
      if Log.isLoggable(System.Logger.Level.TRACE) then
        s"""Error executing query:
           |$query
           |With message:
           |${t.getMessage}
           |And values:
           |${logSqlParams(params)}
           |""".stripMargin
      else s"""Error executing query:
              |$query
              |With message:
              |${t.getMessage}
              |""".stripMargin,
      t
    ):

  def this(frag: Frag, t: Throwable) = this(frag.sqlString, frag.params, t)
end SqlException
