package com.augustnagro.magnum.magzio.example

import com.augustnagro.magnum.magzio.*
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName
import zio.*

import javax.sql.DataSource

object ExampleZioApp extends ZIOAppDefault:

  val postgresContainer: TaskLayer[PostgreSQLContainer] =
    ZLayer.scoped:
      ZIO.fromAutoCloseable:
        ZIO.attemptBlocking:
          val p = PostgreSQLContainer
            .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
            .createContainer()
          p.start()
          p

  val dataSource: URLayer[PostgreSQLContainer, DataSource] =
    ZLayer:
      ZIO.serviceWith(p =>
        val ds = PGSimpleDataSource()
        ds.setUrl(p.jdbcUrl)
        ds.setUser(p.username)
        ds.setPassword(p.password)
        ds
      )

  val transactor: URLayer[DataSource, Transactor] = Transactor.layer

  val layers: TaskLayer[Transactor] =
    ZLayer.make[Transactor](
      postgresContainer,
      dataSource,
      transactor
    )

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    ZIO
      .serviceWithZIO[Transactor] { tx =>
        tx.transact:
          sql"create table if not exists car (id serial primary key, model text, top_speed int, vin int, color text, created timestamp)"
            .update
            .run()

          sql"create table if not exists owner (id serial primary key, name text, car_id int references car(id))"
            .update
            .run()

          val newOwner = ownerRepo.insertReturning:
            OwnerCreator(
              name = "Alice"
            )

          val newCar = carRepo.insertReturning:
            CarCreator(
              model = "Toyota",
              topSpeed = 100,
              vinNumber = Some(123),
              color = Color.Blue
            )

          val updated =
            sql"update owner set car_id = ${newCar.id} where id = ${newOwner.id} and car_id is null".update
              .run()

          if updated > 0 then
            sql"select * from owner join car on owner.car_id = car.id"
              .query[(Owner, Car)]
              .run()
          else throw new NoSuchElementException("Owner not updated.")
      }
      .tap(res => Console.printLine(res.toString))
      .provideLayer(layers)

end ExampleZioApp
