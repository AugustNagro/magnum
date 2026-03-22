package com.augustnagro.magnum.magzio.example

import com.augustnagro.magnum.magzio.*
import com.augustnagro.magnum.*
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import zio.*
import zio.logging.backend.SLF4J

import javax.sql.DataSource

object ExampleZioApp extends ZIOAppDefault:

  private val dataSource: TaskLayer[HikariDataSource] =
    ZLayer.scoped:
      ZIO.fromAutoCloseable:
        ZIO.attempt:
          val ds = new HikariDataSource()
          ds.setJdbcUrl("jdbc:postgresql://localhost:5432/magnum_db")
          ds.setUsername("magnum")
          ds.setPassword("magnum_password")
          ds
      .tap { ds =>
        val config = Flyway.configure
          .loggers("slf4j")
          .dataSource(ds)
          .locations("classpath:migration")

        ZIO.attemptBlocking:
          config.load().migrate()
      }

  private val layers: TaskLayer[TransactorZIO] =
    ZLayer.make[TransactorZIO](
      dataSource,
      TransactorZIO.layer
    )

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    ZIO
      .serviceWithZIO[TransactorZIO] { tx =>
        tx.transact:
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
      .tap(res => ZIO.log(res.toString))
      .provideLayer(layers)

end ExampleZioApp
