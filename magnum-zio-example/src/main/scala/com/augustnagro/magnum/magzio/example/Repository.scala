package com.augustnagro.magnum.magzio.example

import com.augustnagro.magnum.magzio.*
//import com.augustnagro.magnum.pg.PgCodec.given

import java.time.OffsetDateTime

enum Color derives DbCodec:
  case Red, Green, Blue

case class CarCreator(
    model: String,
    topSpeed: Int,
    vinNumber: Option[Int],
    color: Color,
    created: OffsetDateTime = OffsetDateTime.now()
) derives DbCodec

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class Car(
    @Id id: Long,
    model: String,
    topSpeed: Int,
    @SqlName("vin") vinNumber: Option[Int],
//    color: Color,
    color: String,
    created: OffsetDateTime
) derives DbCodec

val carRepo = Repo[CarCreator, Car, Long]
val car = TableInfo[CarCreator, Car, Long]

case class OwnerCreator(
    name: String,
    carId: Option[Long] = None
) derives DbCodec

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class Owner(
    @Id id: Long,
    name: String,
    carId: Option[Long]
) derives DbCodec

val ownerRepo = Repo[OwnerCreator, Owner, Long]
val owner = TableInfo[OwnerCreator, Owner, Long]
