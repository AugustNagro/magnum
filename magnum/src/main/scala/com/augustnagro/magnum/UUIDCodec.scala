package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet, Types}
import java.util.UUID

object UUIDCodec:
  given VarCharUUIDCodec: DbCodec[UUID] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.VARCHAR)
    def readSingle(rs: ResultSet, pos: Int): UUID =
      rs.getString(pos) match
        case null    => null
        case uuidStr => UUID.fromString(uuidStr)
    def writeSingle(entity: UUID, ps: PreparedStatement, pos: Int): Unit =
      ps.setString(pos, entity.toString)
