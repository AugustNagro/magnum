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

  immutableRepoKyoTests(this, PostgresDbType, xa)

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ pgContainer

  def xa(): TransactorKyo =
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

    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    val dsLayer: Layer[DataSource, Any] = Layer(ds)
    val xaLayer = Layer.init[TransactorKyo](TransactorKyo.layer, dsLayer)

    val getXa: TransactorKyo < Env[TransactorKyo] = Env.get[TransactorKyo]

    import AllowUnsafe.embrace.danger
    KyoApp.Unsafe
      .runAndBlock(10.minutes)(
        Memo.run(
          Env.runLayer(xaLayer)(
            getXa
          )
        )
      )
      .getOrThrow
  end xa
end PgZioTests
