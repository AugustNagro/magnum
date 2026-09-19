import com.augustnagro.magnum.*
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource

import java.util.logging.{Level as JulLevel, Logger as JulLogger}

class SqlLoggingTests extends FunSuite:

  test("TRACE logging handles an Option parameter followed by a scalar"):
    val logger = JulLogger.getLogger("com.augustnagro.magnum")
    val previousLevel = logger.getLevel

    try
      logger.setLevel(JulLevel.FINEST)

      val ds = JdbcDataSource()
      ds.setURL("jdbc:h2:mem:sql-logging-tests;DB_CLOSE_DELAY=-1")
      ds.setUser("sa")
      ds.setPassword("")
      val xa = Transactor(ds, SqlLogger.Default)

      xa.connect:
        sql"CREATE TABLE person(id INT PRIMARY KEY, first_name VARCHAR(50))".update
          .run()
        sql"INSERT INTO person VALUES (1, 'George')".update.run()

        val nameFilter: Option[String] = Some("George")
        val minId = 0
        val rows =
          sql"SELECT first_name FROM person WHERE first_name = $nameFilter AND id > $minId"
            .query[String]
            .run()

        assertEquals(rows, Vector("George"))
    finally logger.setLevel(previousLevel)
    end try

end SqlLoggingTests
