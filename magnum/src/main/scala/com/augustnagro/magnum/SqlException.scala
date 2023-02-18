package com.augustnagro.magnum

class SqlException(ex: Exception, sql: Sql)
    extends RuntimeException(
      s"""Error executing query:
         |${sql.query}
         |With message:
         |${ex.getMessage}
         |""".stripMargin,
      ex
    )
