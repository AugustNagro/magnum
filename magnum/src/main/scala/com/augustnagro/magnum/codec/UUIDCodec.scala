package com.augustnagro.magnum.codec

import com.augustnagro.magnum.codec.DbCodec

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

object UUIDCodec:
  given VarCharUUIDCodec: DbCodec[UUID] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.VARCHAR)
    def readSingle(rs: ResultSet, pos: Int): UUID =
      UUID.fromString(rs.getString(pos))
    def readSingleOption(rs: ResultSet, pos: Int): Option[UUID] =
      Option(rs.getString(pos)).map(UUID.fromString)
    def writeSingle(entity: UUID, ps: PreparedStatement, pos: Int): Unit =
      ps.setString(pos, entity.toString)
