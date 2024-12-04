import com.augustnagro.magnum.*
import com.augustnagro.magnum.codec.UUIDCodec.VarCharUUIDCodec
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.OracleDbType
import com.augustnagro.magnum.shared.builders.Transactor
import com.dimafeng.testcontainers.OracleContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite}
import oracle.jdbc.datasource.impl.OracleDataSource
import org.testcontainers.utility.DockerImageName
import shared.*

import java.sql.Statement
import scala.util.Using

class OracleTests extends FunSuite, TestContainersFixtures:

  given DbCodec[Boolean] =
    DbCodec[String].biMap(_ == "Y", b => if b then "Y" else "N")

  sharedTests(this, OracleDbType, xa)

  val oracleContainer = ForAllContainerFixture(
    OracleContainer
      .Def(dockerImageName =
        DockerImageName.parse(
          "gvenzl/oracle-xe:21.3.0"
        )
      )
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ oracleContainer

  def xa(): Transactor =
    val oracle = oracleContainer()
    val ds = OracleDataSource()
    ds.setURL(oracle.jdbcUrl)
    ds.setUser(oracle.username)
    ds.setPassword(oracle.password)
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
          |  created timestamp not null
          |)""".stripMargin
        )
        stmt.execute(
          """insert into car (model, id, top_speed, vin, color, created)
          |values ('McLaren Senna', 1, 208, 123, 'Red', timestamp '2024-11-24 22:17:30')""".stripMargin
        )
        stmt.execute(
          """insert into car (model, id, top_speed, vin, color, created)
          |values ('Ferrari F8 Tributo', 2, 212, 124, 'Green', timestamp '2024-11-24 22:17:31')""".stripMargin
        )
        stmt.execute(
          """insert into car (model, id, top_speed, vin, color, created)
          |values ('Aston Martin Superleggera', 3, 211, null, 'Blue', timestamp '2024-11-24 22:17:32')""".stripMargin
        )
        try stmt.execute("drop table person")
        catch case _ => ()
        stmt.execute(
          """create table person (
          |    id number primary key,
          |    first_name varchar2(50),
          |    last_name varchar2(50) not null,
          |    is_admin varchar2(1) not null,
          |    created timestamp not null,
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
            |  created_at timestamp not null,
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
      )
      .get
    Transactor(ds)
  end xa
end OracleTests
