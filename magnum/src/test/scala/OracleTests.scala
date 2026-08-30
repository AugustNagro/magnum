import com.augustnagro.magnum.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import com.dimafeng.testcontainers.SingleContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite}
import oracle.jdbc.datasource.impl.OracleDataSource
import org.testcontainers.oracle.{OracleContainer as JavaOracleContainer}
import org.testcontainers.containers.wait.strategy.{WaitStrategy, WaitStrategyTarget}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.sql.Statement
import java.time.Duration
import java.time.LocalTime
import scala.util.Using

private object ContainerStartedWait extends WaitStrategy:
  override def waitUntilReady(target: WaitStrategyTarget): Unit = ()

  override def withStartupTimeout(timeout: Duration): WaitStrategy = this

private final class OracleFreeContainer
    extends SingleContainer[JavaOracleContainer]:
  override val container: JavaOracleContainer =
    new JavaOracleContainer(
      DockerImageName.parse("gvenzl/oracle-free:23.26.2-slim-faststart")
    )
      .withUsername("test")
      .withPassword("test")
      .withSharedMemorySize(10240000000L)
      .waitingFor(ContainerStartedWait)
      .withStartupTimeout(Duration.ofSeconds(240))

class OracleTests extends FunSuite, TestContainersFixtures:

  private var oracleReady = false

  given DbCodec[Boolean] =
    DbCodec[String].biMap(_ == "Y", b => if b then "Y" else "N")

  given DbCodec[LocalTime] =
    DbCodec[String].biMap(LocalTime.parse, _.toString)

  sharedTests(this, OracleDbType, xa)

  val oracleContainer = new ForAllContainerFixture(new OracleFreeContainer):
    override def afterContainerStart(container: OracleFreeContainer): Unit =
      awaitOracleReady(container)

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ oracleContainer

  def xa(): Transactor =
    val oracle = oracleContainer()
    awaitOracleReady(oracle)
    val ds = OracleDataSource()
    ds.setURL(jdbcUrl(oracle))
    ds.setUser(oracle.container.getUsername)
    ds.setPassword(oracle.container.getPassword)
    // oracle doesn't support drop if exists,
    // or multi-statement queries
    Using
      .Manager(use =>
        val con = use(ds.getConnection())
        val stmt = use(con.createStatement())
        try stmt.execute("drop table car")
        catch case _ => ()
        stmt.execute(
          """create table car (
          |  model varchar2(50) not null,
          |  id number primary key,
          |  top_speed number not null,
          |  vin number,
          |  color varchar2(50) not null check (color in ('Red', 'Green', 'Blue')),
          |  created timestamp with time zone not null
          |)""".stripMargin
        )

        stmt.execute(
          """insert into car (model, id, top_speed, vin, color, created)
          |values ('McLaren Senna', 1, 208, 123, 'Red', timestamp '2024-11-24 22:17:30 +00:00')""".stripMargin
        )
        stmt.execute(
          """insert into car (model, id, top_speed, vin, color, created)
          |values ('Ferrari F8 Tributo', 2, 212, 124, 'Green', timestamp '2024-11-24 22:17:31 +00:00')""".stripMargin
        )
        stmt.execute(
          """insert into car (model, id, top_speed, vin, color, created)
          |values ('Aston Martin Superleggera', 3, 211, null, 'Blue', timestamp '2024-11-24 22:17:32 +00:00')""".stripMargin
        )
        try stmt.execute("drop table person")
        catch case _ => ()
        stmt.execute(
          """create table person (
          |    id number primary key,
          |    first_name varchar2(50),
          |    last_name varchar2(50) not null,
          |    is_admin varchar2(1) not null,
          |    created timestamp with time zone not null,
          |    social_id varchar2(36)
          |)""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(1, 'George', 'Washington', 'Y', current_timestamp, 'd06443a6-3efb-46c4-a66a-a80a8a9a5388')""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(2, 'Alexander', 'Hamilton', 'Y', current_timestamp, '529b6c6d-7228-4da5-81d7-13b706f78ddb')""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(3, 'John', 'Adams', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(4, 'Benjamin', 'Franklin', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(5, 'John', 'Jay', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(6, 'Thomas', 'Jefferson', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(7, 'James', 'Madison', 'Y', current_timestamp, null)""".stripMargin
        )
        stmt.execute(
          """insert into person (id, first_name, last_name, is_admin, created, social_id) values
          |(8, null, 'Nagro', 'N', current_timestamp, null)""".stripMargin
        )
        try stmt.execute("drop table my_user")
        catch case _ => ()
        stmt.execute(
          """create table my_user (
            |  first_name varchar2(200) not null,
            |  id number generated always as identity,
            |  primary key (id)
            |)
            |""".stripMargin
        )
        stmt.execute("""insert into my_user (first_name) values ('George')""")
        stmt.execute(
          """insert into my_user (first_name) values ('Alexander')"""
        )
        stmt.execute("""insert into my_user (first_name) values ('John')""")
        try stmt.execute("drop table no_id")
        catch case _ => ()
        stmt.execute(
          """create table no_id (
            |  created_at timestamp with time zone not null,
            |  user_name varchar2(200) not null,
            |  user_action varchar2(200) not null
            |)
            |""".stripMargin
        )
        stmt.execute(
          """insert into no_id (created_at, user_name, user_action) values
            |(timestamp '1997-08-15 00:00:00', 'Josh', 'clicked a button')""".stripMargin
        )
        stmt.execute(
          """insert into no_id (created_at, user_name, user_action) values
            |(timestamp '1997-08-16 00:00:00', 'Danny', 'opened a toaster')""".stripMargin
        )
        stmt.execute(
          """insert into no_id (created_at, user_name, user_action) values
            |(timestamp '1997-08-17 00:00:00', 'Greg', 'ran some QA tests')""".stripMargin
        )
        try stmt.execute("drop table big_dec")
        catch case _ => ()
        stmt.execute(
          """create table big_dec (
            |  id number primary key,
            |  my_big_dec numeric
            |)""".stripMargin
        )
        stmt.execute("insert into big_dec (id, my_big_dec) values (1, 123)")
        stmt.execute("insert into big_dec (id, my_big_dec) values (2, null)")
        try stmt.execute("drop table my_time")
        catch case _ => ()
        stmt.execute(
          """create table my_time (
            |  a timestamp with time zone not null,
            |  b date not null,
            |  c VARCHAR2(100) not null,
            |  d timestamp not null
            |)
            |""".stripMargin
        )
        stmt.execute(
          "insert into my_time values (timestamp '2025-03-30 21:19:23 -00:00', date '2025-03-30', '05:20:04', timestamp '2025-04-02 20:16:38')"
        )
        stmt.execute(
          "insert into my_time values (timestamp '2025-03-31 21:19:23 -00:00', date '2025-03-31', '05:30:04', timestamp '2025-04-02 20:17:38')"
        )
      )
      .get
    Transactor(ds)
  end xa

  private def jdbcUrl(oracle: OracleFreeContainer): String =
    s"jdbc:oracle:thin:@${oracle.host}:${oracle.mappedPort(1521)}/FREEPDB1"

  private def awaitOracleReady(oracle: OracleFreeContainer): Unit =
    if !oracleReady then this.synchronized {
      if !oracleReady then
        val ds = OracleDataSource()
        ds.setURL(jdbcUrl(oracle))
        ds.setUser(oracle.container.getUsername)
        ds.setPassword(oracle.container.getPassword)
        ds.setLoginTimeout(5)
        val deadline = System.nanoTime() + Duration.ofSeconds(240).toNanos()
        var consecutiveSuccesses = 0
        while consecutiveSuccesses < 5 && System.nanoTime() < deadline do
          try
            val querySucceeded = Using.Manager { use =>
              val connection = use(ds.getConnection())
              val statement = use(connection.createStatement())
              val result = use(statement.executeQuery("select 1 from dual"))
              result.next() && result.getInt(1) == 1
            }.get
            if querySucceeded then consecutiveSuccesses += 1
            else consecutiveSuccesses = 0
          catch
            case _: Exception => consecutiveSuccesses = 0
          if consecutiveSuccesses < 5 then Thread.sleep(1000)
        if consecutiveSuccesses < 5 then
          throw IllegalStateException("Timed out waiting for Oracle Free to finish initializing")
        oracleReady = true
    }
end OracleTests
