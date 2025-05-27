package com.augustnagro.magnum.pg

import com.augustnagro.magnum.DbCodec
import org.postgresql.geometric.{
  PGbox,
  PGcircle,
  PGline,
  PGlseg,
  PGpath,
  PGpoint,
  PGpolygon
}
import org.postgresql.util.PGInterval

import java.sql
import java.sql.{JDBCType, PreparedStatement, ResultSet, Types}
import scala.reflect.ClassTag
import scala.collection.{IterableFactory, mutable as m}
import scala.compiletime.*

object PgCodec:

  inline given ArrayCodec[A](using
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A],
      cTag: ClassTag[Array[A]]
  ): DbCodec[Array[A]] =
    inline erasedValue[A] match
      // https://jdbc.postgresql.org/documentation/server-prepare/#arrays
      case _: Short | _: java.lang.Short | _: Int | _: java.lang.Integer |
          _: Long | _: java.lang.Long | _: Float | _: java.lang.Float |
          _: Double | _: java.lang.Double | _: Boolean | _: java.lang.Boolean |
          _: String | _: IArray[Byte] | _: Array[Byte] =>
        arrayFastPath(aCodec, aArrayCodec, cTag)
      case _ =>
        arraySlowPath(aCodec, aArrayCodec, cTag)

  inline given IArrayCodec[A](using
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A],
      cTag: ClassTag[IArray[A]]
  ): DbCodec[IArray[A]] =
    inline erasedValue[A] match
      // https://jdbc.postgresql.org/documentation/server-prepare/#arrays
      case _: Short | _: java.lang.Short | _: Int | _: java.lang.Integer |
          _: Long | _: java.lang.Long | _: Float | _: java.lang.Float |
          _: Double | _: java.lang.Double | _: Boolean | _: java.lang.Boolean |
          _: String | _: IArray[Byte] | _: Array[Byte] =>
        iArrayFastPath(aCodec, aArrayCodec, cTag)
      case _ =>
        iArraySlowPath(aCodec, aArrayCodec, cTag)

  given SeqCodec[A](using
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A]
  ): DbCodec[Seq[A]] = new DbCodec[Seq[A]]:
    require(aCodec.cols.length == 1)
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.OTHER)
    def readSingle(resultSet: ResultSet, pos: Int): Seq[A] =
      readCImpl(Seq, resultSet, pos)
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[Seq[A]] =
      readCOptImpl(Seq, resultSet, pos)
    def writeSingle(entity: Seq[A], ps: PreparedStatement, pos: Int): Unit =
      writeCImpl(entity, ps, pos)

  given ListCodec[A](using
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A]
  ): DbCodec[List[A]] = new DbCodec[List[A]]:
    require(aCodec.cols.length == 1)
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.OTHER)
    def readSingle(resultSet: ResultSet, pos: Int): List[A] =
      readCImpl(List, resultSet, pos)
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[List[A]] =
      readCOptImpl(List, resultSet, pos)
    def writeSingle(entity: List[A], ps: PreparedStatement, pos: Int): Unit =
      writeCImpl(entity, ps, pos)

  given VectorCodec[A](using
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A]
  ): DbCodec[Vector[A]] = new DbCodec[Vector[A]]:
    require(aCodec.cols.length == 1)
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.OTHER)
    def readSingle(resultSet: ResultSet, pos: Int): Vector[A] =
      readCImpl(Vector, resultSet, pos)
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[Vector[A]] =
      readCOptImpl(Vector, resultSet, pos)
    def writeSingle(entity: Vector[A], ps: PreparedStatement, pos: Int): Unit =
      writeCImpl(entity, ps, pos)

  given BufferCodec[A](using
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A]
  ): DbCodec[m.Buffer[A]] = new DbCodec[m.Buffer[A]]:
    require(aCodec.cols.length == 1)
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.OTHER)
    def readSingle(resultSet: ResultSet, pos: Int): m.Buffer[A] =
      readCImpl(m.Buffer, resultSet, pos)
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[m.Buffer[A]] =
      readCOptImpl(m.Buffer, resultSet, pos)
    def writeSingle(
        entity: m.Buffer[A],
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      writeCImpl(entity, ps, pos)

  given PgBoxCodec: DbCodec[PGbox] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGbox =
      resultSet.getObject(pos, classOf[PGbox])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGbox] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGbox, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgCircleCodec: DbCodec[PGcircle] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGcircle =
      resultSet.getObject(pos, classOf[PGcircle])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGcircle] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGcircle, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgIntervalCodec: DbCodec[PGInterval] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGInterval =
      resultSet.getObject(pos, classOf[PGInterval])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGInterval] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGInterval, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgLineCodec: DbCodec[PGline] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGline =
      resultSet.getObject(pos, classOf[PGline])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGline] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGline, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgLSegCodec: DbCodec[PGlseg] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGlseg =
      resultSet.getObject(pos, classOf[PGlseg])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGlseg] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGlseg, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgPathCodec: DbCodec[PGpath] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGpath =
      resultSet.getObject(pos, classOf[PGpath])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGpath] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGpath, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgPointCodec: DbCodec[PGpoint] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGpoint =
      resultSet.getObject(pos, classOf[PGpoint])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGpoint] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGpoint, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given PgPolygonCodec: DbCodec[PGpolygon] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(resultSet: ResultSet, pos: Int): PGpolygon =
      resultSet.getObject(pos, classOf[PGpolygon])
    def readSingleOption(resultSet: ResultSet, pos: Int): Option[PGpolygon] =
      readOptImpl(this, resultSet, pos)
    def writeSingle(entity: PGpolygon, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  private def iArrayFastPath[A](
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A],
      cTag: ClassTag[IArray[A]]
  ): DbCodec[IArray[A]] = new DbCodec[IArray[A]]:
    require(aCodec.cols.length == 1)

    def queryRepr: String = "?"

    val cols: IArray[Int] = IArray(Types.OTHER)

    def readSingle(resultSet: ResultSet, pos: Int): IArray[A] =
      val jdbcArray = resultSet.getArray(pos)
      try
        val arr = aArrayCodec.readArray(jdbcArray.getArray)
        IArray.unsafeFromArray(arr)
      finally jdbcArray.free()

    def readSingleOption(resultSet: ResultSet, pos: Int): Option[IArray[A]] =
      val jdbcArray = resultSet.getArray(pos)
      if resultSet.wasNull then None
      else
        try
          val arr = aArrayCodec.readArray(jdbcArray.getArray)
          Some(IArray.unsafeFromArray(arr))
        finally jdbcArray.free()

    def writeSingle(entity: IArray[A], ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  private def iArraySlowPath[A](
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A],
      cTag: ClassTag[IArray[A]]
  ): DbCodec[IArray[A]] = new DbCodec[IArray[A]]:
    require(aCodec.cols.length == 1)

    def queryRepr: String = "?"

    val cols: IArray[Int] = IArray(Types.OTHER)

    def readSingle(resultSet: ResultSet, pos: Int): IArray[A] =
      val jdbcArray = resultSet.getArray(pos)
      try
        val arr = aArrayCodec.readArray(jdbcArray.getArray)
        IArray.unsafeFromArray(arr)
      finally jdbcArray.free()

    def readSingleOption(resultSet: ResultSet, pos: Int): Option[IArray[A]] =
      val jdbcArray = resultSet.getArray(pos)
      if resultSet.wasNull then None
      else
        try
          val arr = aArrayCodec.readArray(jdbcArray.getArray)
          Some(IArray.unsafeFromArray(arr))
        finally jdbcArray.free()

    def writeSingle(entity: IArray[A], ps: PreparedStatement, pos: Int): Unit =
      val arr = entity.iterator.map(aArrayCodec.toArrayObj).toArray
      val jdbcArr =
        ps.getConnection.createArrayOf(aArrayCodec.jdbcTypeName, arr)
      ps.setArray(pos, jdbcArr)

  private def arrayFastPath[A](
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A],
      cTag: ClassTag[Array[A]]
  ): DbCodec[Array[A]] = new DbCodec[Array[A]]:
    require(aCodec.cols.length == 1)

    def queryRepr: String = "?"

    val cols: IArray[Int] = IArray(Types.OTHER)

    def readSingle(resultSet: ResultSet, pos: Int): Array[A] =
      val jdbcArray = resultSet.getArray(pos)
      try aArrayCodec.readArray(jdbcArray.getArray)
      finally jdbcArray.free()

    def readSingleOption(resultSet: ResultSet, pos: Int): Option[Array[A]] =
      val jdbcArray = resultSet.getArray(pos)
      if resultSet.wasNull then None
      else
        try Some(aArrayCodec.readArray(jdbcArray.getArray))
        finally jdbcArray.free()

    def writeSingle(entity: Array[A], ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  private def arraySlowPath[A](
      aCodec: DbCodec[A],
      aArrayCodec: SqlArrayCodec[A],
      cTag: ClassTag[Array[A]]
  ): DbCodec[Array[A]] = new DbCodec[Array[A]]:
    require(aCodec.cols.length == 1)

    def queryRepr: String = "?"

    val cols: IArray[Int] = IArray(Types.OTHER)

    def readSingle(resultSet: ResultSet, pos: Int): Array[A] =
      val jdbcArray = resultSet.getArray(pos)
      try aArrayCodec.readArray(jdbcArray.getArray)
      finally jdbcArray.free()

    def readSingleOption(resultSet: ResultSet, pos: Int): Option[Array[A]] =
      val jdbcArray = resultSet.getArray(pos)
      if resultSet.wasNull then None
      else
        try Some(aArrayCodec.readArray(jdbcArray.getArray))
        finally jdbcArray.free()

    def writeSingle(entity: Array[A], ps: PreparedStatement, pos: Int): Unit =
      val arr = entity.iterator.map(aArrayCodec.toArrayObj).toArray
      val jdbcArr =
        ps.getConnection.createArrayOf(aArrayCodec.jdbcTypeName, arr)
      ps.setArray(pos, jdbcArr)

  private inline def readCImpl[C[_], A](
      factory: IterableFactory[C],
      resultSet: ResultSet,
      pos: Int
  )(using arrayCodec: SqlArrayCodec[A]): C[A] =
    val jdbcArray = resultSet.getArray(pos)
    try
      val arr = arrayCodec.readArray(jdbcArray.getArray)
      factory.from(arr)
    finally jdbcArray.free()

  private inline def readCOptImpl[C[_], A](
      factory: IterableFactory[C],
      resultSet: ResultSet,
      pos: Int
  )(using arrayCodec: SqlArrayCodec[A]): Option[C[A]] =
    val jdbcArray = resultSet.getArray(pos)
    if resultSet.wasNull then None
    else
      try
        val arr = arrayCodec.readArray(jdbcArray.getArray)
        Some(factory.from(arr))
      finally jdbcArray.free()

  private inline def writeCImpl[C[_], A](
      entity: Iterable[A],
      ps: PreparedStatement,
      pos: Int
  )(using arrayCodec: SqlArrayCodec[A]): Unit =
    val arr = entity.iterator.map(arrayCodec.toArrayObj).toArray
    val jdbcArr =
      ps.getConnection.createArrayOf(arrayCodec.jdbcTypeName, arr)
    ps.setArray(pos, jdbcArr)

  private inline def readOptImpl[A](
      codec: DbCodec[A],
      resultSet: ResultSet,
      pos: Int
  ): Option[A] =
    val res = codec.readSingle(resultSet, pos)
    if resultSet.wasNull then None
    else Some(res)

end PgCodec
