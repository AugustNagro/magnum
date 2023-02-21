package com.augustnagro.magnum

class SqlException(t: Throwable, sql: Sql)
    extends RuntimeException(
      s"""Error executing query:
         |${sql.query}
         |With message:
         |${t.getMessage}
         |""".stripMargin,
      t
    )
