package com.augustnagro.magnum

class SqlException(query: String, params: Iterable[Any], t: Throwable = null)
    extends RuntimeException(
      if Log.isLoggable(System.Logger.Level.TRACE) then
        s"""Error executing query:
           |$query
           |With message:
           |${t.getMessage}
           |And values:
           |${params.mkString("[", ", ", "]")}
           |""".stripMargin
      else s"""Error executing query:
           |$query
           |With message:
           |${t.getMessage}
           |""".stripMargin,
      t
    )
