package com.augustnagro.magnum.pg

import java.sql
import java.sql.JDBCType
import java.time.{OffsetDateTime, ZoneOffset}
import scala.reflect.ClassTag
import scala.collection.mutable as m

/** Typeclass for converting between raw JDBC Object arrays and type A */
trait SqlArrayCodec[A]:
  def jdbcTypeName: String

  /** Converts the raw JDBC array to an IArray[A] */
  def readArray(array: Object): Array[A]

  /** Maps entity A to an object for use in JDBC Array */
  def toArrayObj(entity: A): Object

object SqlArrayCodec:

  given AnySqlArrayCodec: SqlArrayCodec[Any] with
    val jdbcTypeName: String = JDBCType.JAVA_OBJECT.getName
    def readArray(array: Object): Array[Any] =
      array.asInstanceOf[Array[Any]]
    def toArrayObj(entity: Any): Object = entity.asInstanceOf[Object]

  given StringSqlArrayCodec: SqlArrayCodec[String] with
    val jdbcTypeName: String = JDBCType.VARCHAR.getName
    def readArray(array: Object): Array[String] =
      array.asInstanceOf[Array[String]]
    def toArrayObj(entity: String): Object = entity

  given BooleanSqlArrayCodec: SqlArrayCodec[Boolean] with
    val jdbcTypeName: String = JDBCType.BOOLEAN.getName
    def readArray(array: Object): Array[Boolean] =
      array match
        case boxed: Array[java.lang.Boolean] => boxed.map(Boolean.unbox)
        case primitive: Array[Boolean]       => primitive
    def toArrayObj(entity: Boolean): Object = Boolean.box(entity)

  given ByteSqlArrayCodec: SqlArrayCodec[Byte] with
    val jdbcTypeName: String = JDBCType.TINYINT.getName
    def readArray(array: Object): Array[Byte] =
      array match
        case boxed: Array[java.lang.Byte] => boxed.map(Byte.unbox)
        case primitive: Array[Byte]       => primitive
    def toArrayObj(entity: Byte): Object = Byte.box(entity)

  given ShortSqlArrayCodec: SqlArrayCodec[Short] with
    val jdbcTypeName: String = JDBCType.SMALLINT.getName
    def readArray(array: Object): Array[Short] =
      array match
        case boxed: Array[java.lang.Short] => boxed.map(Short.unbox)
        case primitive: Array[Short]       => primitive
    def toArrayObj(entity: Short): Object = Short.box(entity)

  given IntSqlArrayCodec: SqlArrayCodec[Int] with
    val jdbcTypeName: String = JDBCType.INTEGER.getName
    def readArray(array: Object): Array[Int] =
      array match
        case boxed: Array[Integer] => boxed.map(Int.unbox)
        case primitive: Array[Int] => primitive
    def toArrayObj(entity: Int): Object = Int.box(entity)

  given LongSqlArrayCodec: SqlArrayCodec[Long] with
    val jdbcTypeName: String = JDBCType.BIGINT.getName
    def readArray(array: Object): Array[Long] =
      array match
        case boxed: Array[java.lang.Long] => boxed.map(Long.unbox)
        case primitive: Array[Long]       => primitive
    def toArrayObj(entity: Long): Object = Long.box(entity)

  given FloatSqlArrayCodec: SqlArrayCodec[Float] with
    val jdbcTypeName: String = JDBCType.REAL.getName
    def readArray(array: Object): Array[Float] =
      array match
        case boxed: Array[java.lang.Float] => boxed.map(Float.unbox)
        case primitive: Array[Float]       => primitive
    def toArrayObj(entity: Float): Object = Float.box(entity)

  given DoubleSqlArrayCodec: SqlArrayCodec[Double] with
    val jdbcTypeName: String = JDBCType.DOUBLE.getName
    def readArray(array: Object): Array[Double] =
      array match
        case boxed: Array[java.lang.Double] => boxed.map(Double.unbox)
        case primitive: Array[Double]       => primitive
    def toArrayObj(entity: Double): Object = Double.box(entity)

  given SqlDateSqlArrayCodec: SqlArrayCodec[sql.Date] with
    val jdbcTypeName: String = JDBCType.DATE.getName
    def readArray(array: Object): Array[sql.Date] =
      array.asInstanceOf[Array[sql.Date]]
    def toArrayObj(entity: sql.Date): Object = entity

  given SqlTimeSqlArrayCodec: SqlArrayCodec[sql.Time] with
    val jdbcTypeName: String = JDBCType.TIME.getName
    def readArray(array: Object): Array[sql.Time] =
      array.asInstanceOf[Array[sql.Time]]
    def toArrayObj(entity: sql.Time): Object = entity

  given SqlTimestampSqlArrayCodec: SqlArrayCodec[sql.Timestamp] with
    val jdbcTypeName: String = JDBCType.TIMESTAMP.getName
    def readArray(array: Object): Array[sql.Timestamp] =
      array.asInstanceOf[Array[sql.Timestamp]]
    def toArrayObj(entity: sql.Timestamp): Object = entity

  given OffsetDateTimeSqlArrayCodec: SqlArrayCodec[OffsetDateTime] with
    val jdbcTypeName: String = JDBCType.TIMESTAMP.getName
    def readArray(array: Object): Array[OffsetDateTime] =
      array
        .asInstanceOf[Array[sql.Timestamp]]
        .map(_.toInstant.atOffset(ZoneOffset.UTC))
    def toArrayObj(entity: OffsetDateTime): Object =
      sql.Timestamp.from(entity.toInstant)

  given ArraySqlArrayCodec[A](using
      aCodec: SqlArrayCodec[A],
      cTag: ClassTag[Array[A]]
  ): SqlArrayCodec[Array[A]] with
    def jdbcTypeName: String = aCodec.jdbcTypeName
    def readArray(array: Object): Array[Array[A]] =
      val objArr = array.asInstanceOf[Array[Object]]
      objArr.map(aCodec.readArray)
    def toArrayObj(entity: Array[A]): Object =
      entity.iterator.map(aCodec.toArrayObj).toArray

  given IArraySqlArrayCodec[A](using
      aCodec: SqlArrayCodec[A],
      cTag: ClassTag[IArray[A]]
  ): SqlArrayCodec[IArray[A]] with
    def jdbcTypeName: String = aCodec.jdbcTypeName
    def readArray(array: Object): Array[IArray[A]] =
      val objArray = array.asInstanceOf[Array[Object]]
      objArray.map(obj => IArray.unsafeFromArray(aCodec.readArray(obj)))
    def toArrayObj(entity: IArray[A]): Object =
      entity.iterator.map(aCodec.toArrayObj).toArray

  given SeqSqlArrayCodec[A](using
      aCodec: SqlArrayCodec[A]
  ): SqlArrayCodec[Seq[A]] with
    def jdbcTypeName: String = aCodec.jdbcTypeName
    def readArray(array: Object): Array[Seq[A]] =
      val objArray = array.asInstanceOf[Array[Object]]
      objArray.map(obj => Seq.from(aCodec.readArray(obj)))
    def toArrayObj(entity: Seq[A]): Object =
      entity.iterator.map(aCodec.toArrayObj).toArray

  given ListSqlArrayCodec[A](using
      aCodec: SqlArrayCodec[A]
  ): SqlArrayCodec[List[A]] with
    def jdbcTypeName: String = aCodec.jdbcTypeName
    def readArray(array: Object): Array[List[A]] =
      val objArray = array.asInstanceOf[Array[Object]]
      objArray.map(obj => List.from(aCodec.readArray(obj)))
    def toArrayObj(entity: List[A]): Object =
      entity.iterator.map(aCodec.toArrayObj).toArray

  given VectorSqlArrayCodec[A](using
      aCodec: SqlArrayCodec[A]
  ): SqlArrayCodec[Vector[A]] with
    def jdbcTypeName: String = aCodec.jdbcTypeName
    def readArray(array: Object): Array[Vector[A]] =
      val objArr = array.asInstanceOf[Array[Object]]
      objArr.map(obj => Vector.from(aCodec.readArray(obj)))
    def toArrayObj(entity: Vector[A]): Object =
      entity.iterator.map(aCodec.toArrayObj).toArray

  given BufferSqlArrayCodec[A](using
      aCodec: SqlArrayCodec[A]
  ): SqlArrayCodec[m.Buffer[A]] with
    def jdbcTypeName: String = aCodec.jdbcTypeName
    def readArray(array: Object): Array[m.Buffer[A]] =
      val objArray = array.asInstanceOf[Array[Object]]
      objArray.map(obj => m.Buffer.from(aCodec.readArray(obj)))
    def toArrayObj(entity: m.Buffer[A]): Object =
      entity.iterator.map(aCodec.toArrayObj).toArray
end SqlArrayCodec
