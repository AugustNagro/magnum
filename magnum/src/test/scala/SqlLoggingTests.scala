import com.augustnagro.magnum.*
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource

import java.util.logging.{
  Handler,
  Level as JulLevel,
  Logger as JulLogger,
  LogRecord
}
import scala.collection.mutable.ArrayBuffer

class SqlLoggingTests extends FunSuite:

  @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
  case class LoggedPerson(id: Long, name: String) derives DbCodec

  private def transactor(dbName: String, sqlLogger: SqlLogger): Transactor =
    val ds = JdbcDataSource()
    ds.setURL(s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1")
    ds.setUser("sa")
    ds.setPassword("")
    Transactor(ds, sqlLogger)

  private def withTrace[A](f: => A): A =
    val logger = JulLogger.getLogger("com.augustnagro.magnum")
    val previousLevel = logger.getLevel
    try
      logger.setLevel(JulLevel.FINEST)
      f
    finally logger.setLevel(previousLevel)

  private def withCapturedLogs[A](f: ArrayBuffer[String] => A): A =
    val logger = JulLogger.getLogger("com.augustnagro.magnum")
    val messages = ArrayBuffer.empty[String]
    val handler = new Handler:
      def publish(record: LogRecord): Unit = messages.append(record.getMessage)
      def flush(): Unit = ()
      def close(): Unit = ()
    handler.setLevel(JulLevel.FINEST)
    logger.addHandler(handler)
    try withTrace(f(messages))
    finally logger.removeHandler(handler)

  test("TRACE logging handles an Option parameter followed by a scalar"):
    withCapturedLogs: messages =>
      val xa = transactor("sql-logging-option", SqlLogger.Default)
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
        assert(
          messages.exists(message =>
            message.contains("SELECT first_name FROM person") &&
              message.contains("With values:\n(Some(George), 0)")
          )
        )

  test("batch logging keeps scalar and product inputs as separate rows"):
    withCapturedLogs: messages =>
      val events = ArrayBuffer.empty[SqlSuccessEvent]
      val capturingLogger = new SqlLogger:
        def log(event: SqlSuccessEvent): Unit =
          events.append(event)
          SqlLogger.Default.log(event)
        def exceptionMsg(event: SqlExceptionEvent): String =
          SqlLogger.Default.exceptionMsg(event)

      val xa = transactor("sql-logging-batch", capturingLogger)
      val repo = Repo[LoggedPerson, LoggedPerson, Long]
      val people = Vector(LoggedPerson(1L, "George"), LoggedPerson(2L, "Ada"))

      xa.connect:
        sql"CREATE TABLE logged_person(id BIGINT PRIMARY KEY, name VARCHAR(50))".update
          .run()
        events.clear()
        messages.clear()

        repo.insertAll(people)
        repo.deleteAllById(people.map(_.id))

      assertEquals(events.size, 2)
      val inserted = Vector(Vector[Any](1L, "George"), Vector[Any](2L, "Ada"))
      val deleted = Vector(Vector[Any](1L), Vector[Any](2L))
      assertEquals(events(0).params.map(_.toVector).toVector, inserted)
      assertEquals(events(1).params.map(_.toVector).toVector, deleted)
      assertEquals(events(0).params.map(_.toVector).toVector, inserted)
      assert(
        messages.exists(message =>
          message.contains("INSERT INTO logged_person") &&
            message.contains("With values:\n(1, George),\n(2, Ada)")
        )
      )
      assert(
        messages.exists(message =>
          message.contains("DELETE FROM logged_person") &&
            message.contains("With values:\n(1),\n(2)")
        )
      )

  test("TRACE exception messages include a mixed Option and scalar row"):
    withTrace:
      val xa = transactor("sql-logging-exception", SqlLogger.Default)
      xa.connect:
        sql"CREATE TABLE person(id INT PRIMARY KEY, first_name VARCHAR(50))".update
          .run()
        sql"INSERT INTO person VALUES (1, 'George')".update.run()

        val nameFilter: Option[String] = Some("George")
        val duplicateId = 1
        val error = intercept[SqlException]:
          sql"INSERT INTO person(first_name, id) VALUES ($nameFilter, $duplicateId)".update
            .run()

        assert(error.getMessage.contains("And values:"))
        assert(error.getMessage.contains("(Some(George), 1)"))

end SqlLoggingTests
