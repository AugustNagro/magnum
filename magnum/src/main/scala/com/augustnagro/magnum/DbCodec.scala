package com.augustnagro.magnum

import java.net.URL
import java.sql.{JDBCType, PreparedStatement, ResultSet, Types}
import java.time.{
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  ZoneId,
  ZoneOffset
}
import java.util.UUID
import scala.annotation.implicitNotFound
import scala.deriving.Mirror
import scala.compiletime.{
  constValue,
  constValueTuple,
  erasedValue,
  error,
  summonFrom,
  summonInline
}
import scala.quoted.*
import scala.reflect.ClassTag

/** Typeclass for JDBC reading & writing.
  */
trait DbCodec[E]:
  self =>

  /** Syntax used when querying the db. For example,
    *
    * DbCodec[Int].queryRepr == "?"
    *
    * DbCodec[(String, Boolean)].queryRepr = "(?, ?)"
    *
    * case class User(id: Long, name: String) derives DbCodec
    * DbCodec[User].queryRepr = "? ?"
    */
  def queryRepr: String

  /** The `java.sql.Types` constant for every "?" in `queryRepr`. For mapping
    * database-specific types, Types.JAVA_OBJECT is recommended.
    */
  def cols: IArray[Int]

  /** Read an E from the ResultSet starting at position `pos` and ending after
    * reading `cols` number of columns. Make sure the ResultSet is in a valid
    * state (ie, ResultSet::next has been called).
    */
  def readSingle(resultSet: ResultSet, pos: Int): E

  /** Build an E from the ResultSet starting at position 1 and ending after
    * reading `cols` number of columns. Make sure the ResultSet is in a valid
    * state (ie, ResultSet::next has been called).
    */
  def readSingle(resultSet: ResultSet): E = readSingle(resultSet, 1)

  /** Read an Option[E] from the ResultSet starting at position `pos` and ending
    * after reading `cols` number of columns. Make sure the ResultSet is in a
    * valid state (ie, ResultSet::next has been called).
    */
  def readSingleOption(resultSet: ResultSet, pos: Int): Option[E]

  /** Build every row in the ResultSet into a sequence of E. The ResultSet
    * should be in its initial position before calling (ie, ResultSet::next not
    * called).
    */
  def read(resultSet: ResultSet): Vector[E] =
    val res = Vector.newBuilder[E]
    while resultSet.next() do res += readSingle(resultSet)
    res.result()

  /** Write the entity to the PreparedStatement starting at position `pos` */
  def writeSingle(entity: E, ps: PreparedStatement, pos: Int): Unit

  /** Write the entity to the resultSet starting at position 1 */
  def writeSingle(entity: E, ps: PreparedStatement): Unit =
    writeSingle(entity, ps, 1)

  /** Writes multiple entities to the preparedStatement via
    * PreparedStatement::addBatch
    */
  def write(entities: Iterable[E], ps: PreparedStatement): Unit =
    for e <- entities do
      writeSingle(e, ps)
      ps.addBatch()

  def biMap[E2](to: E => E2, from: E2 => E): DbCodec[E2] =
    new DbCodec[E2]:
      val cols: IArray[Int] = self.cols
      def readSingle(rs: ResultSet, pos: Int): E2 =
        to(self.readSingle(rs, pos))
      def readSingleOption(rs: ResultSet, pos: Int): Option[E2] =
        self.readSingleOption(rs, pos).map(to)
      def writeSingle(e: E2, ps: PreparedStatement, pos: Int): Unit =
        self.writeSingle(from(e), ps, pos)
      def queryRepr: String = self.queryRepr
end DbCodec

object DbCodec:

  inline def apply[E](using codec: DbCodec[E]): DbCodec[E] = codec

  given AnyCodec: DbCodec[Any] with
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(rs: ResultSet, pos: Int): Any = rs.getObject(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Any] =
      Option(rs.getObject(pos))
    def writeSingle(a: Any, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, a)
    def queryRepr: String = "?"

  given StringCodec: DbCodec[String] with
    val cols: IArray[Int] = IArray(Types.VARCHAR)
    def readSingle(rs: ResultSet, pos: Int): String = rs.getString(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[String] =
      Option(rs.getString(pos))
    def writeSingle(s: String, ps: PreparedStatement, pos: Int): Unit =
      ps.setString(pos, s)
    def queryRepr: String = "?"

  given BooleanCodec: DbCodec[Boolean] with
    val cols: IArray[Int] = IArray(Types.BOOLEAN)
    def readSingle(rs: ResultSet, pos: Int): Boolean = rs.getBoolean(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Boolean] =
      readOptImpl(this, rs, pos)
    def writeSingle(b: Boolean, ps: PreparedStatement, pos: Int): Unit =
      ps.setBoolean(pos, b)
    def queryRepr: String = "?"

  given ByteCodec: DbCodec[Byte] with
    val cols: IArray[Int] = IArray(Types.TINYINT)
    def readSingle(rs: ResultSet, pos: Int): Byte = rs.getByte(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Byte] =
      readOptImpl(this, rs, pos)
    def writeSingle(b: Byte, ps: PreparedStatement, pos: Int): Unit =
      ps.setByte(pos, b)
    def queryRepr: String = "?"

  given ShortCodec: DbCodec[Short] with
    val cols: IArray[Int] = IArray(Types.SMALLINT)
    def readSingle(rs: ResultSet, pos: Int): Short = rs.getShort(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Short] =
      readOptImpl(this, rs, pos)
    def writeSingle(s: Short, ps: PreparedStatement, pos: Int): Unit =
      ps.setShort(pos, s)
    def queryRepr: String = "?"

  given IntCodec: DbCodec[Int] with
    val cols: IArray[Int] = IArray(Types.INTEGER)
    def readSingle(rs: ResultSet, pos: Int): Int = rs.getInt(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Int] =
      readOptImpl(this, rs, pos)
    def writeSingle(i: Int, ps: PreparedStatement, pos: Int): Unit =
      ps.setInt(pos, i)
    def queryRepr: String = "?"

  given LongCodec: DbCodec[Long] with
    val cols: IArray[Int] = IArray(Types.BIGINT)
    def readSingle(rs: ResultSet, pos: Int): Long = rs.getLong(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Long] =
      readOptImpl(this, rs, pos)
    def writeSingle(l: Long, ps: PreparedStatement, pos: Int): Unit =
      ps.setLong(pos, l)
    def queryRepr: String = "?"

  given FloatCodec: DbCodec[Float] with
    val cols: IArray[Int] = IArray(Types.REAL)
    def readSingle(rs: ResultSet, pos: Int): Float = rs.getFloat(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Float] =
      readOptImpl(this, rs, pos)
    def writeSingle(f: Float, ps: PreparedStatement, pos: Int): Unit =
      ps.setFloat(pos, f)
    def queryRepr: String = "?"

  given DoubleCodec: DbCodec[Double] with
    val cols: IArray[Int] = IArray(Types.DOUBLE)
    def readSingle(rs: ResultSet, pos: Int): Double = rs.getDouble(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Double] =
      readOptImpl(this, rs, pos)
    def writeSingle(d: Double, ps: PreparedStatement, pos: Int): Unit =
      ps.setDouble(pos, d)
    def queryRepr: String = "?"

  given ByteArrayCodec: DbCodec[Array[Byte]] with
    val cols: IArray[Int] = IArray(Types.BINARY)
    def readSingle(rs: ResultSet, pos: Int): Array[Byte] = rs.getBytes(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Array[Byte]] =
      Option(rs.getBytes(pos))
    def writeSingle(bytes: Array[Byte], ps: PreparedStatement, pos: Int): Unit =
      ps.setBytes(pos, bytes)
    def queryRepr: String = "?"

  given ByteIArrayCodec: DbCodec[IArray[Byte]] with
    val cols: IArray[Int] = IArray(Types.BINARY)
    def readSingle(rs: ResultSet, pos: Int): IArray[Byte] =
      IArray.unsafeFromArray(rs.getBytes(pos))
    def readSingleOption(rs: ResultSet, pos: Int): Option[IArray[Byte]] =
      ByteArrayCodec.readSingleOption(rs, pos).map(IArray.unsafeFromArray)
    def writeSingle(
        bytes: IArray[Byte],
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setBytes(pos, IArray.genericWrapArray(bytes).toArray)
    def queryRepr: String = "?"

  given SqlDateCodec: DbCodec[java.sql.Date] with
    val cols: IArray[Int] = IArray(Types.DATE)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Date = rs.getDate(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.Date] =
      Option(rs.getDate(pos))
    def writeSingle(
        date: java.sql.Date,
        ps: PreparedStatement,
        pos: Int
    ): Unit = ps.setDate(pos, date)
    def queryRepr: String = "?"

  given SqlTimeCodec: DbCodec[java.sql.Time] with
    val cols: IArray[Int] = IArray(Types.TIME)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Time =
      rs.getTime(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.Time] =
      Option(rs.getTime(pos))
    def writeSingle(
        time: java.sql.Time,
        ps: PreparedStatement,
        pos: Int
    ): Unit = ps.setTime(pos, time)
    def queryRepr: String = "?"

  given SqlTimestampCodec: DbCodec[java.sql.Timestamp] with
    val cols: IArray[Int] = IArray(Types.TIMESTAMP)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Timestamp =
      rs.getTimestamp(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.Timestamp] =
      Option(rs.getTimestamp(pos))
    def writeSingle(
        t: java.sql.Timestamp,
        ps: PreparedStatement,
        pos: Int
    ): Unit = ps.setTimestamp(pos, t)
    def queryRepr: String = "?"

  given OffsetDateTimeCodec: DbCodec[OffsetDateTime] with
    val cols: IArray[Int] = IArray(Types.TIMESTAMP_WITH_TIMEZONE)
    def readSingle(rs: ResultSet, pos: Int): OffsetDateTime =
      rs.getObject(pos, classOf[OffsetDateTime])
    def readSingleOption(rs: ResultSet, pos: Int): Option[OffsetDateTime] =
      readOptImpl(this, rs, pos)
    def writeSingle(dt: OffsetDateTime, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, dt)
    def queryRepr: String = "?"

  given InstantCodec: DbCodec[Instant] =
    OffsetDateTimeCodec.biMap(_.toInstant, _.atOffset(ZoneOffset.UTC))

  given LocalDateCodec: DbCodec[LocalDate] with
    val cols: IArray[Int] = IArray(Types.DATE)
    def readSingle(rs: ResultSet, pos: Int): LocalDate =
      rs.getObject(pos, classOf[LocalDate])
    def readSingleOption(rs: ResultSet, pos: Int): Option[LocalDate] =
      readOptImpl(this, rs, pos)
    def writeSingle(ld: LocalDate, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, ld)
    def queryRepr: String = "?"

  given LocalTimeCodec: DbCodec[LocalTime] with
    val cols: IArray[Int] = IArray(Types.TIME)
    def readSingle(rs: ResultSet, pos: Int): LocalTime =
      rs.getObject(pos, classOf[LocalTime])
    def readSingleOption(rs: ResultSet, pos: Int): Option[LocalTime] =
      readOptImpl(this, rs, pos)
    def writeSingle(lt: LocalTime, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, lt)
    def queryRepr: String = "?"

  given LocalDateTimeCodec: DbCodec[LocalDateTime] with
    val cols: IArray[Int] = IArray(Types.TIMESTAMP)
    def readSingle(rs: ResultSet, pos: Int): LocalDateTime =
      rs.getObject(pos, classOf[LocalDateTime])
    def readSingleOption(rs: ResultSet, pos: Int): Option[LocalDateTime] =
      readOptImpl(this, rs, pos)
    def writeSingle(ldt: LocalDateTime, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, ldt)
    def queryRepr: String = "?"

  given ZoneIdCodec: DbCodec[ZoneId] =
    StringCodec.biMap(ZoneId.of, _.toString)

  given SqlRefCodec: DbCodec[java.sql.Ref] with
    val cols: IArray[Int] = IArray(Types.REF)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Ref = rs.getRef(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.Ref] =
      readOptImpl(this, rs, pos)
    def writeSingle(ref: java.sql.Ref, ps: PreparedStatement, pos: Int): Unit =
      ps.setRef(pos, ref)
    def queryRepr: String = "?"

  given SqlBlobCodec: DbCodec[java.sql.Blob] with
    val cols: IArray[Int] = IArray(Types.BLOB)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Blob = rs.getBlob(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.Blob] =
      readOptImpl(this, rs, pos)
    def writeSingle(b: java.sql.Blob, ps: PreparedStatement, pos: Int): Unit =
      ps.setBlob(pos, b)
    def queryRepr: String = "?"

  given SqlClobCodec: DbCodec[java.sql.Clob] with
    val cols: IArray[Int] = IArray(Types.CLOB)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Clob = rs.getClob(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.Clob] =
      readOptImpl(this, rs, pos)
    def writeSingle(c: java.sql.Clob, ps: PreparedStatement, pos: Int): Unit =
      ps.setClob(pos, c)
    def queryRepr: String = "?"

  given URLCodec: DbCodec[URL] with
    val cols: IArray[Int] = IArray(Types.VARCHAR)
    def readSingle(rs: ResultSet, pos: Int): URL = rs.getURL(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[URL] =
      Option(rs.getURL(pos))
    def writeSingle(url: URL, ps: PreparedStatement, pos: Int): Unit =
      ps.setURL(pos, url)
    def queryRepr: String = "?"

  given RowIdCodec: DbCodec[java.sql.RowId] with
    val cols: IArray[Int] = IArray(Types.ROWID)
    def readSingle(rs: ResultSet, pos: Int): java.sql.RowId = rs.getRowId(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.RowId] =
      Option(rs.getRowId(pos))
    def writeSingle(
        rowId: java.sql.RowId,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setRowId(pos, rowId)
    def queryRepr: String = "?"

  given SqlNClobCodec: DbCodec[java.sql.NClob] with
    val cols: IArray[Int] = IArray(Types.NCLOB)
    def readSingle(rs: ResultSet, pos: Int): java.sql.NClob = rs.getNClob(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.NClob] =
      readOptImpl(this, rs, pos)
    def writeSingle(nc: java.sql.NClob, ps: PreparedStatement, pos: Int): Unit =
      ps.setNClob(pos, nc)
    def queryRepr: String = "?"

  given SqlXmlCodec: DbCodec[java.sql.SQLXML] with
    val cols: IArray[Int] = IArray(Types.SQLXML)
    def readSingle(rs: ResultSet, pos: Int): java.sql.SQLXML = rs.getSQLXML(pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[java.sql.SQLXML] =
      readOptImpl(this, rs, pos)
    def writeSingle(s: java.sql.SQLXML, ps: PreparedStatement, pos: Int): Unit =
      ps.setSQLXML(pos, s)
    def queryRepr: String = "?"

  given JavaBigDecimalCodec: DbCodec[java.math.BigDecimal] with
    val cols: IArray[Int] = IArray(Types.NUMERIC)
    def readSingle(rs: ResultSet, pos: Int): java.math.BigDecimal =
      rs.getBigDecimal(pos)
    def readSingleOption(
        rs: ResultSet,
        pos: Int
    ): Option[java.math.BigDecimal] =
      Option(rs.getBigDecimal(pos))
    def writeSingle(
        bd: java.math.BigDecimal,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setBigDecimal(pos, bd)
    def queryRepr: String = "?"

  given ScalaBigDecimalCodec: DbCodec[scala.math.BigDecimal] with
    val cols: IArray[Int] = IArray(Types.NUMERIC)
    def readSingle(rs: ResultSet, pos: Int): scala.math.BigDecimal =
      scala.math.BigDecimal(rs.getBigDecimal(pos))
    def readSingleOption(rs: ResultSet, pos: Int): Option[BigDecimal] =
      JavaBigDecimalCodec
        .readSingleOption(rs, pos)
        .map(scala.math.BigDecimal.apply)
    def writeSingle(
        bd: scala.math.BigDecimal,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setBigDecimal(pos, bd.underlying)
    def queryRepr: String = "?"

  given UUIDCodec: DbCodec[UUID] with
    def queryRepr: String = "?"
    val cols: IArray[Int] = IArray(Types.OTHER)
    def readSingle(rs: ResultSet, pos: Int): UUID =
      rs.getObject(pos, classOf[UUID])
    def readSingleOption(rs: ResultSet, pos: Int): Option[UUID] =
      val res = rs.getObject(pos, classOf[UUID])
      if rs.wasNull then None
      else Some(res)
    def writeSingle(entity: UUID, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, entity)

  given OptionCodec[A](using codec: DbCodec[A]): DbCodec[Option[A]] with
    def cols: IArray[Int] = codec.cols
    def readSingle(rs: ResultSet, pos: Int): Option[A] =
      codec.readSingleOption(rs, pos)
    def readSingleOption(rs: ResultSet, pos: Int): Option[Option[A]] =
      Some(codec.readSingleOption(rs, pos))
    def writeSingle(opt: Option[A], ps: PreparedStatement, pos: Int): Unit =
      opt match
        case Some(a) =>
          codec.writeSingle(a, ps, pos)
        case None =>
          for i <- cols.indices do ps.setNull(pos + i, cols(i))
    def queryRepr: String = codec.queryRepr

  given Tuple2Codec[A, B](using
      aCodec: DbCodec[A],
      bCodec: DbCodec[B]
  ): DbCodec[(A, B)] with
    val cols: IArray[Int] = IArray.concat(aCodec.cols, bCodec.cols)
    def readSingle(rs: ResultSet, pos: Int): (A, B) = (
      aCodec.readSingle(rs, pos),
      bCodec.readSingle(rs, pos + aCodec.cols.length)
    )
    def readSingleOption(rs: ResultSet, pos: Int): Option[(A, B)] =
      val a = aCodec.readSingleOption(rs, pos)
      val b = bCodec.readSingleOption(rs, pos + aCodec.cols.length)
      (a, b) match
        case (Some(a), Some(b)) => Some((a, b))
        case _                  => None
    def writeSingle(tup: (A, B), ps: PreparedStatement, pos: Int): Unit =
      aCodec.writeSingle(tup._1, ps, pos)
      bCodec.writeSingle(tup._2, ps, pos + aCodec.cols.length)
    val queryRepr: String = s"(${aCodec.queryRepr}, ${bCodec.queryRepr})"

  given Tuple3Codec[A, B, C](using
      aCodec: DbCodec[A],
      bCodec: DbCodec[B],
      cCodec: DbCodec[C]
  ): DbCodec[(A, B, C)] with
    val cols: IArray[Int] =
      IArray.concat(aCodec.cols, bCodec.cols, cCodec.cols)
    def readSingle(rs: ResultSet, pos: Int): (A, B, C) =
      var i = pos
      val a = aCodec.readSingle(rs, i)
      i += aCodec.cols.length
      val b = bCodec.readSingle(rs, i)
      i += bCodec.cols.length
      val c = cCodec.readSingle(rs, i)
      (a, b, c)
    def readSingleOption(rs: ResultSet, pos: Int): Option[(A, B, C)] =
      var i = pos
      val a = aCodec.readSingleOption(rs, i)
      i += aCodec.cols.length
      val b = bCodec.readSingleOption(rs, i)
      i += bCodec.cols.length
      val c = cCodec.readSingleOption(rs, i)
      (a, b, c) match
        case (Some(a), Some(b), Some(c)) => Some((a, b, c))
        case _                           => None
    def writeSingle(tup: (A, B, C), ps: PreparedStatement, pos: Int): Unit =
      var i = pos
      aCodec.writeSingle(tup._1, ps, i)
      i += aCodec.cols.length
      bCodec.writeSingle(tup._2, ps, i)
      i += bCodec.cols.length
      cCodec.writeSingle(tup._3, ps, i)
    val queryRepr: String =
      s"(${aCodec.queryRepr}, ${bCodec.queryRepr}, ${cCodec.queryRepr})"
  end Tuple3Codec

  given Tuple4Codec[A, B, C, D](using
      aCodec: DbCodec[A],
      bCodec: DbCodec[B],
      cCodec: DbCodec[C],
      dCodec: DbCodec[D]
  ): DbCodec[(A, B, C, D)] with
    val cols: IArray[Int] =
      IArray.concat(aCodec.cols, bCodec.cols, cCodec.cols, dCodec.cols)
    def readSingle(rs: ResultSet, pos: Int): (A, B, C, D) =
      var i = pos
      val a = aCodec.readSingle(rs, i)
      i += aCodec.cols.length
      val b = bCodec.readSingle(rs, i)
      i += bCodec.cols.length
      val c = cCodec.readSingle(rs, i)
      i += cCodec.cols.length
      val d = dCodec.readSingle(rs, i)
      (a, b, c, d)
    def readSingleOption(rs: ResultSet, pos: Int): Option[(A, B, C, D)] =
      var i = pos
      val a = aCodec.readSingleOption(rs, i)
      i += aCodec.cols.length
      val b = bCodec.readSingleOption(rs, i)
      i += bCodec.cols.length
      val c = cCodec.readSingleOption(rs, i)
      i += cCodec.cols.length
      val d = dCodec.readSingleOption(rs, i)
      (a, b, c, d) match
        case (Some(a), Some(b), Some(c), Some(d)) => Some((a, b, c, d))
        case _                                    => None
    def writeSingle(tup: (A, B, C, D), ps: PreparedStatement, pos: Int): Unit =
      var i = pos
      aCodec.writeSingle(tup._1, ps, i)
      i += aCodec.cols.length
      bCodec.writeSingle(tup._2, ps, i)
      i += bCodec.cols.length
      cCodec.writeSingle(tup._3, ps, i)
      i += cCodec.cols.length
      dCodec.writeSingle(tup._4, ps, i)
    val queryRepr: String =
      s"(${aCodec.queryRepr}, ${bCodec.queryRepr}, ${cCodec.queryRepr}, ${dCodec.queryRepr})"
  end Tuple4Codec

  private inline def readOptImpl[A](
      codec: DbCodec[A],
      resultSet: ResultSet,
      pos: Int
  ): Option[A] =
    val res = codec.readSingle(resultSet, pos)
    if resultSet.wasNull then None
    else Some(res)

  inline given derived[E: Mirror.Of]: DbCodec[E] =
    ${ dbCodecImpl[E] }

  private def dbCodecImpl[E: Type](using Quotes): Expr[DbCodec[E]] =
    import quotes.reflect.*
    val mirror = Expr.summon[Mirror.Of[E]].getOrElse {
      report.errorAndAbort(
        "Can only derive DbCodec for case classes, sealed traits or enums (products and sums)."
      )
    }
    mirror match
      case '{
            $mp: Mirror.ProductOf[E] {
              type MirroredElemTypes = mets
            }
          } =>
        val colsExpr = buildColsExpr[mets]()
        '{
          new DbCodec[E] {
            val cols: IArray[Int] = $colsExpr
            def readSingle(rs: ResultSet, pos: Int): E =
              ${
                productReadSingle[E, mets]('{ rs }, mp, Vector.empty, '{ pos })
              }
            def readSingleOption(rs: ResultSet, pos: Int): Option[E] =
              ${
                productReadOption[E, mets]('{ rs }, mp, Vector.empty, '{ pos })
              }
            def writeSingle(e: E, ps: PreparedStatement, pos: Int): Unit =
              ${
                productWriteSingle[E, mets]('{ e }, '{ ps }, '{ pos }, '{ 0 })
              }
            val queryRepr: String = ${ productQueryRepr[mets]() }
          }
        }
      case '{
            $ms: Mirror.SumOf[E] {
              type MirroredElemTypes = mets
              type MirroredElemLabels = mels
              type MirroredLabel = mel
            }
          } =>
        val nameMapExpr = DerivingUtil.buildSqlNameMapForEnum[E, mels, mets]
        val melExpr = Expr(Type.valueOfConstant[mel].get.toString)
        '{
          new DbCodec[E] {
            val nameMap: Seq[(String, E)] = $nameMapExpr
            val cols: IArray[Int] = IArray(Types.VARCHAR)
            def readSingle(rs: ResultSet, pos: Int): E =
              val str = rs.getString(pos)
              nameMap.find((name, _) => name == str) match
                case Some((_, v)) => v
                case None =>
                  throw IllegalArgumentException(
                    str + " not convertible to " + $melExpr
                  )
            def readSingleOption(rs: ResultSet, pos: Int): Option[E] =
              Option(rs.getString(pos)).map(str =>
                nameMap.find((name, _) => name == str) match
                  case Some((_, v)) => v
                  case None =>
                    throw IllegalArgumentException(
                      str + " not convertible to " + $melExpr
                    )
              )
            def writeSingle(entity: E, ps: PreparedStatement, pos: Int): Unit =
              nameMap.find((_, v) => v == entity) match
                case Some((k, _)) => ps.setString(pos, k)
                case None =>
                  throw IllegalArgumentException(
                    entity.toString + " not convertible to " + $melExpr
                  )
            def queryRepr: String = "?"
          }
        }
    end match
  end dbCodecImpl

  private def productQueryRepr[Mets: Type](
      elemReprs: Vector[Expr[String]] = Vector.empty
  )(using Quotes): Expr[String] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        Expr.summon[DbCodec[met]] match
          case Some(codec) =>
            productQueryRepr[metTail](elemReprs :+ '{ $codec.queryRepr })
          case None =>
            productQueryRepr[metTail](elemReprs :+ '{ "?" })
      case '[EmptyTuple] =>
        val seqExpr = Expr.ofSeq(elemReprs)
        '{ $seqExpr.mkString(", ") }

  private def buildColsExpr[Mets: Type](
      res: Vector[Expr[IArray[Int]]] = Vector.empty
  )(using Quotes): Expr[IArray[Int]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val metCodec = Expr.summon[DbCodec[met]].getOrElse {
          val metType = TypeRepr.of[met].show
          report.errorAndAbort(
            s"Cannot find a DbCodec instance for $metType! Provide one or derive it."
          )
        }
        val newCols = '{ $metCodec.cols }
        buildColsExpr[metTail](res :+ newCols)
      case '[EmptyTuple] =>
        '{
          val iArrays: Seq[IArray[Int]] = ${ Expr.ofSeq(res) }
          IArray.concat(iArrays*)
        }

  private def productReadSingle[E: Type, Mets: Type](
      rs: Expr[ResultSet],
      m: Expr[Mirror.ProductOf[E]],
      res: Vector[Expr[Any]],
      pos: Expr[Int]
  )(using Quotes): Expr[E] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        Expr.summon[DbCodec[met]] match
          case Some(codecExpr) =>
            '{
              val posValue = $pos
              val codec = $codecExpr
              val metValue = codec.readSingle($rs, posValue)
              val newPos = posValue + codec.cols.length
              ${
                productReadSingle[E, metTail](
                  rs,
                  m,
                  res :+ '{ metValue },
                  '{ newPos }
                )
              }
            }
          case None =>
            Expr.summon[ClassTag[met]] match
              case Some(clsTagExpr) =>
                report.info(
                  s"Could not find DbCodec for ${TypeRepr.of[met].show}. Defaulting to ResultSet::[get|set]Object"
                )
                '{
                  val posValue = $pos
                  val metValue = $rs.getObject(
                    posValue,
                    $clsTagExpr.runtimeClass.asInstanceOf[Class[met]]
                  )
                  val newPos = posValue + 1
                  ${
                    productReadSingle[E, metTail](
                      rs,
                      m,
                      res :+ '{ metValue },
                      '{ newPos }
                    )
                  }
                }
              case None =>
                report.errorAndAbort(
                  "Could not find DbCodec or ClassTag for ${TypeRepr.of[met].show}"
                )
      case '[EmptyTuple] =>
        '{
          val product = ${ Expr.ofTupleFromSeq(res) }
          $m.fromProduct(product)
        }
    end match
  end productReadSingle

  private def productReadOption[E: Type, Mets: Type](
      rs: Expr[ResultSet],
      m: Expr[Mirror.ProductOf[E]],
      res: Vector[Expr[Any]],
      pos: Expr[Int]
  )(using Quotes): Expr[Option[E]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        Expr.summon[DbCodec[met]] match
          case Some(codecExpr) =>
            '{
              val posValue = $pos
              val codec = $codecExpr
              codec.readSingleOption($rs, posValue) match
                case Some(metValue) =>
                  val newPos = posValue + codec.cols.length
                  ${
                    productReadOption[E, metTail](
                      rs,
                      m,
                      res :+ '{ metValue },
                      '{ newPos }
                    )
                  }
                case None => None
            }
          case None =>
            Expr.summon[ClassTag[met]] match
              case Some(clsTagExpr) =>
                report.info(
                  s"Could not find DbCodec for ${TypeRepr.of[met].show}. Defaulting to ResultSet::[get|set]Object"
                )
                '{
                  val posValue = $pos
                  val metValue = $rs.getObject(
                    posValue,
                    $clsTagExpr.runtimeClass.asInstanceOf[Class[met]]
                  )
                  if $rs.wasNull then None
                  else
                    val newPos = posValue + 1
                    ${
                      productReadOption[E, metTail](
                        rs,
                        m,
                        res :+ '{ metValue },
                        '{ newPos }
                      )
                    }
                }
              case None =>
                report.errorAndAbort(
                  "Could not find DbCodec or ClassTag for ${TypeRepr.of[met].show}"
                )
      case '[EmptyTuple] =>
        '{
          val product = ${ Expr.ofTupleFromSeq(res) }
          Some($m.fromProduct(product))
        }
    end match
  end productReadOption

  private def productWriteSingle[E: Type, Mets: Type](
      e: Expr[E],
      ps: Expr[PreparedStatement],
      pos: Expr[Int],
      i: Expr[Int]
  )(using Quotes): Expr[Unit] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        Expr.summon[DbCodec[met]] match
          case Some(codecExpr) =>
            '{
              val iValue = $i
              val posValue = $pos
              val metValue = $e
                .asInstanceOf[Product]
                .productElement(iValue)
                .asInstanceOf[met]
              val codec = $codecExpr
              codec.writeSingle(metValue, $ps, posValue)
              val newPos = posValue + $codecExpr.cols.length
              val newI = iValue + 1
              ${ productWriteSingle[E, metTail](e, ps, '{ newPos }, '{ newI }) }
            }
          case None =>
            '{
              val iValue = $i
              val posValue = $pos
              val metValue = $e
                .asInstanceOf[Product]
                .productElement(iValue)
              $ps.setObject(posValue, metValue)
              val newPos = posValue + 1
              val newI = iValue + 1
              ${ productWriteSingle[E, metTail](e, ps, '{ newPos }, '{ newI }) }
            }
      case '[EmptyTuple] => '{}
    end match
  end productWriteSingle
end DbCodec
