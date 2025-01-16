package com.augustnagro.magnum.magcats

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import scala.util.Using
import scala.util.Using.Manager
import com.augustnagro.magnum.magcats.immutableRepoCatsEffectTests
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.augustnagro.magnum.SqlLogger
import cats.effect.kernel.Resource
import cats.syntax.all.*

class PgCatsEffectTests extends FunSuite, TestContainersFixtures:

  given IORuntime = IORuntime.global

  immutableRepoCatsEffectTests(this, PostgresDbType, xa)

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[_]] =
    super.munitFixtures :+ pgContainer

  def xa(): Transactor[IO] =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    val tableDDLs = Vector(
      "/pg/car.sql",
      "/pg/person.sql",
      "/pg/my-user.sql",
      "/pg/no-id.sql",
      "/pg/big-dec.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))

    val setup =
      Resource
        .fromAutoCloseable(IO.delay(ds.getConnection))
        .use: con =>
          Resource
            .fromAutoCloseable(IO.delay(con.createStatement))
            .use: stmt =>
              tableDDLs.traverse_(ddl => IO.delay(stmt.execute(ddl)))
                .flatMap: _ =>
                  Transactor[IO](ds)

    setup.unsafeRunSync()
  end xa
end PgCatsEffectTests
