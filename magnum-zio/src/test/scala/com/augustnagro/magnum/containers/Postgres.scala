package com.augustnagro.magnum.containers

import org.testcontainers.containers.PostgreSQLContainer

import javax.sql.DataSource

import zio.*
import org.postgresql.ds.PGSimpleDataSource
import com.augustnagro.magnum.magzio.Transactor

case class ContainerConfig(
    initScriptPath: String = "pg/init.sql",
    imageName: String = s"${PostgreSQLContainer.IMAGE}:latest"
):
  println(this)
object ContainerConfig:
  val default = ZLayer.succeed(ContainerConfig())

case class Postgres(
    config: ContainerConfig
):
  val postgres: PostgreSQLContainer[?] =
    val container: PostgreSQLContainer[?] =
      new PostgreSQLContainer(config.imageName)
        // Disable password checks
        .withEnv("POSTGRES_HOST_AUTH_METHOD", "trust")
    container.withInitScript(config.initScriptPath)
    container

  def start: Postgres =
    postgres.start()
    this

  val stop: UIO[Unit] =
    ZIO.succeed:
      postgres.stop()

object Postgres:
  val base = ZLayer.derive[Postgres]
  val layer = base >>> ZLayer.scoped:
    ZIO.acquireRelease(ZIO.service[Postgres].map(_.start))(_.stop)
  val default = ContainerConfig.default >>> layer

case class DataSourceProvider(container: Postgres):
  import container.postgres

  def dataSource =
    val ds = PGSimpleDataSource()
    ds.setURL(postgres.getJdbcUrl())
    ds.setUser(postgres.getUsername())
    ds.setPassword(postgres.getPassword())
    ds

object DataSourceProvider:
  private val base = ZLayer.derive[DataSourceProvider]
  val datasource: URLayer[Postgres, DataSource] =
    base.flatMap(l => ZLayer.succeed(l.get.dataSource))
  val transactor: ZLayer[Postgres, Nothing, Transactor] =
    DataSourceProvider.datasource.flatMap: ds =>
      com.augustnagro.magnum.magzio.Transactor.layer(ds.get)
  val defaultTransactor = Postgres.default >>> transactor
