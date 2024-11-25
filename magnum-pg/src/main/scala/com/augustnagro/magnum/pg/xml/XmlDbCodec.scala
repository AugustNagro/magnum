package com.augustnagro.magnum.pg.xml

import com.augustnagro.magnum.DbCodec
import org.postgresql.util.PGobject

import java.sql.{PreparedStatement, ResultSet, Types}

trait XmlDbCodec[A] extends DbCodec[A]:

  def encode(a: A): String

  def decode(xml: String): A

  override def queryRepr: String = "?"

  override val cols: IArray[Int] = IArray(Types.SQLXML)

  override def readSingle(resultSet: ResultSet, pos: Int): A =
    decode(resultSet.getString(pos))

  override def readSingleOption(resultSet: ResultSet, pos: Int): Option[A] =
    val xmlString = resultSet.getString(pos)
    if xmlString == null then None
    else Some(decode(xmlString))

  override def writeSingle(entity: A, ps: PreparedStatement, pos: Int): Unit =
    val xmlObject = PGobject()
    xmlObject.setType("xml")
    xmlObject.setValue(encode(entity))
    ps.setObject(pos, xmlObject)

end XmlDbCodec
