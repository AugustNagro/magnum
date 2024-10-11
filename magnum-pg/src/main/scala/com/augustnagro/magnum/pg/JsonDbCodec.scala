package com.augustnagro.magnum.pg

import com.augustnagro.magnum.DbCodec
import org.postgresql.util.PGobject

import java.sql.{PreparedStatement, ResultSet, Types}

trait JsonDbCodec[A] extends DbCodec[A]:

  def encode(a: A): String

  def decode(json: String): A

  override def queryRepr: String = "?::json"

  override val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)

  override def readSingle(resultSet: ResultSet, pos: Int): A =
    val rawJson = resultSet.getString(pos)
    if rawJson == null then null.asInstanceOf[A]
    else decode(rawJson)

  override def writeSingle(entity: A, ps: PreparedStatement, pos: Int): Unit =
    val jsonObject = PGobject()
    jsonObject.setType("json")
    val encoded = if entity == null then null else encode(entity)
    jsonObject.setValue(encoded)
    ps.setObject(pos, jsonObject)

end JsonDbCodec
