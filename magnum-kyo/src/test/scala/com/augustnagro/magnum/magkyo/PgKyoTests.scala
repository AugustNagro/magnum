package com.augustnagro.magnum.magkyo

import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName
import kyo.*

import java.nio.file.{Files, Path}
import scala.util.Using
import scala.util.Using.Manager
import javax.sql.DataSource

class PgZioTests extends FunSuite, TestContainersFixtures:

  immutableRepoKyoTests(this, PostgresDbType, transactorKyo)

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ pgContainer

  def transactorKyo: TransactorKyo < (Resource & IO) =
    for
      pg <- IO(pgContainer())
      ds <- IO {
        val init = PGSimpleDataSource()
        init.setUrl(pg.jdbcUrl)
        init.setUser(pg.username)
        init.setPassword(pg.password)
        init
      }
      tableDDLs <- Kyo.foreach(
        List(
          "/pg/car.sql",
          "/pg/person.sql",
          "/pg/my-user.sql",
          "/pg/no-id.sql",
          "/pg/big-dec.sql"
        )
      )(p => IO(Files.readString(Path.of(getClass.getResource(p).toURI))))
      conn <- Resource.acquireRelease(IO(ds.getConnection()))(con =>
        IO(con.close())
      )
      stmt <- Resource.acquireRelease(conn.createStatement())(stmt =>
        IO(stmt.close())
      )
      _ <- Kyo.foreach(tableDDLs)(ddl => IO(stmt.execute(ddl)))
      xa <- TransactorKyo.make(ds, SqlLogger.Default, _ => (), Maybe.Absent)
    yield xa
end PgZioTests
