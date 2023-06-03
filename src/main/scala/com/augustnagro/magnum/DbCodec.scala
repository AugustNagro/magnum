package com.augustnagro.magnum

import java.net.URL
import java.sql.{JDBCType, PreparedStatement, ResultSet, Types}
import java.time.{LocalDate, LocalDateTime, LocalTime}
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

  /** The `java.sql.Types` constant for every column of entity E. For mapping
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

object DbCodec:

  inline def apply[E](using codec: DbCodec[E]): DbCodec[E] = codec

  given AnyCodec: DbCodec[Any] with
    val cols: IArray[Int] = IArray(Types.JAVA_OBJECT)
    def readSingle(rs: ResultSet, pos: Int): Any = rs.getObject(pos)
    def writeSingle(a: Any, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, a)

  given StringReader: DbCodec[String] with
    val cols: IArray[Int] = IArray(Types.VARCHAR)
    def readSingle(rs: ResultSet, pos: Int): String = rs.getString(pos)
    def writeSingle(s: String, ps: PreparedStatement, pos: Int): Unit =
      ps.setString(pos, s)

  given BooleanReader: DbCodec[Boolean] with
    val cols: IArray[Int] = IArray(Types.BOOLEAN)
    def readSingle(rs: ResultSet, pos: Int): Boolean = rs.getBoolean(pos)
    def writeSingle(b: Boolean, ps: PreparedStatement, pos: Int): Unit =
      ps.setBoolean(pos, b)

  given ByteReader: DbCodec[Byte] with
    val cols: IArray[Int] = IArray(Types.TINYINT)
    def readSingle(rs: ResultSet, pos: Int): Byte = rs.getByte(pos)
    def writeSingle(b: Byte, ps: PreparedStatement, pos: Int): Unit =
      ps.setByte(pos, b)

  given ShortReader: DbCodec[Short] with
    val cols: IArray[Int] = IArray(Types.SMALLINT)
    def readSingle(rs: ResultSet, pos: Int): Short = rs.getShort(pos)
    def writeSingle(s: Short, ps: PreparedStatement, pos: Int): Unit =
      ps.setShort(pos, s)

  given IntReader: DbCodec[Int] with
    val cols: IArray[Int] = IArray(Types.INTEGER)
    def readSingle(rs: ResultSet, pos: Int): Int = rs.getInt(pos)
    def writeSingle(i: Int, ps: PreparedStatement, pos: Int): Unit =
      ps.setInt(pos, i)

  given LongReader: DbCodec[Long] with
    val cols: IArray[Int] = IArray(Types.BIGINT)
    def readSingle(rs: ResultSet, pos: Int): Long = rs.getLong(pos)
    def writeSingle(l: Long, ps: PreparedStatement, pos: Int): Unit =
      ps.setLong(pos, l)

  given FloatReader: DbCodec[Float] with
    val cols: IArray[Int] = IArray(Types.REAL)
    def readSingle(rs: ResultSet, pos: Int): Float = rs.getFloat(pos)
    def writeSingle(f: Float, ps: PreparedStatement, pos: Int): Unit =
      ps.setFloat(pos, f)

  given DoubleReader: DbCodec[Double] with
    val cols: IArray[Int] = IArray(Types.DOUBLE)
    def readSingle(rs: ResultSet, pos: Int): Double = rs.getDouble(pos)
    def writeSingle(d: Double, ps: PreparedStatement, pos: Int): Unit =
      ps.setDouble(pos, d)

  given ByteArrayReader: DbCodec[Array[Byte]] with
    val cols: IArray[Int] = IArray(Types.BINARY)
    def readSingle(rs: ResultSet, pos: Int): Array[Byte] = rs.getBytes(pos)
    def writeSingle(bytes: Array[Byte], ps: PreparedStatement, pos: Int): Unit =
      ps.setBytes(pos, bytes)

  given ByteIArrayReader: DbCodec[IArray[Byte]] with
    val cols: IArray[Int] = IArray(Types.BINARY)
    def readSingle(rs: ResultSet, pos: Int): IArray[Byte] =
      IArray.unsafeFromArray(rs.getBytes(pos))
    def writeSingle(
        bytes: IArray[Byte],
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setBytes(pos, bytes.toArray)

  given SqlDateReader: DbCodec[java.sql.Date] with
    val cols: IArray[Int] = IArray(Types.DATE)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Date = rs.getDate(pos)
    def writeSingle(
        date: java.sql.Date,
        ps: PreparedStatement,
        pos: Int
    ): Unit = ps.setDate(pos, date)

  given SqlTimeReader: DbCodec[java.sql.Time] with
    val cols: IArray[Int] = IArray(Types.TIME)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Time =
      rs.getTime(pos)
    def writeSingle(
        time: java.sql.Time,
        ps: PreparedStatement,
        pos: Int
    ): Unit = ps.setTime(pos, time)

  given SqlTimestampReader: DbCodec[java.sql.Timestamp] with
    val cols: IArray[Int] = IArray(Types.TIMESTAMP)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Timestamp =
      rs.getTimestamp(pos)
    def writeSingle(
        t: java.sql.Timestamp,
        ps: PreparedStatement,
        pos: Int
    ): Unit = ps.setTimestamp(pos, t)

  given SqlRefReader: DbCodec[java.sql.Ref] with
    val cols: IArray[Int] = IArray(Types.REF)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Ref = rs.getRef(pos)
    def writeSingle(ref: java.sql.Ref, ps: PreparedStatement, pos: Int): Unit =
      ps.setRef(pos, ref)

  given SqlBlobReader: DbCodec[java.sql.Blob] with
    val cols: IArray[Int] = IArray(Types.BLOB)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Blob = rs.getBlob(pos)
    def writeSingle(b: java.sql.Blob, ps: PreparedStatement, pos: Int): Unit =
      ps.setBlob(pos, b)

  given SqlClobReader: DbCodec[java.sql.Clob] with
    val cols: IArray[Int] = IArray(Types.CLOB)
    def readSingle(rs: ResultSet, pos: Int): java.sql.Clob = rs.getClob(pos)
    def writeSingle(c: java.sql.Clob, ps: PreparedStatement, pos: Int): Unit =
      ps.setClob(pos, c)

  given URLReader: DbCodec[URL] with
    val cols: IArray[Int] = IArray(Types.VARCHAR)
    def readSingle(rs: ResultSet, pos: Int): URL = rs.getURL(pos)
    def writeSingle(url: URL, ps: PreparedStatement, pos: Int): Unit =
      ps.setURL(pos, url)

  given RowIdReader: DbCodec[java.sql.RowId] with
    val cols: IArray[Int] = IArray(Types.ROWID)
    def readSingle(rs: ResultSet, pos: Int): java.sql.RowId = rs.getRowId(pos)
    def writeSingle(
        rowId: java.sql.RowId,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setRowId(pos, rowId)

  given SqlNClobReader: DbCodec[java.sql.NClob] with
    val cols: IArray[Int] = IArray(Types.NCLOB)
    def readSingle(rs: ResultSet, pos: Int): java.sql.NClob = rs.getNClob(pos)
    def writeSingle(nc: java.sql.NClob, ps: PreparedStatement, pos: Int): Unit =
      ps.setNClob(pos, nc)

  given SqlXmlReader: DbCodec[java.sql.SQLXML] with
    val cols: IArray[Int] = IArray(Types.SQLXML)
    def readSingle(rs: ResultSet, pos: Int): java.sql.SQLXML = rs.getSQLXML(pos)
    def writeSingle(s: java.sql.SQLXML, ps: PreparedStatement, pos: Int): Unit =
      ps.setSQLXML(pos, s)

  given JavaBigDecimalReader: DbCodec[java.math.BigDecimal] with
    val cols: IArray[Int] = IArray(Types.NUMERIC)
    def readSingle(rs: ResultSet, pos: Int): java.math.BigDecimal =
      rs.getBigDecimal(pos)
    def writeSingle(
        bd: java.math.BigDecimal,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setBigDecimal(pos, bd)

  given ScalaBigDecimalReader: DbCodec[scala.math.BigDecimal] with
    val cols: IArray[Int] = IArray(Types.NUMERIC)
    def readSingle(rs: ResultSet, pos: Int): scala.math.BigDecimal =
      scala.math.BigDecimal(rs.getBigDecimal(pos))
    def writeSingle(
        bd: scala.math.BigDecimal,
        ps: PreparedStatement,
        pos: Int
    ): Unit =
      ps.setBigDecimal(pos, bd.underlying)

  given OptionReader[A](using codec: DbCodec[A]): DbCodec[Option[A]] with
    def cols: IArray[Int] = codec.cols
    def readSingle(rs: ResultSet, pos: Int): Option[A] =
      Option(codec.readSingle(rs, pos))
    def writeSingle(opt: Option[A], ps: PreparedStatement, pos: Int): Unit =
      opt match
        case Some(a) =>
          codec.writeSingle(a, ps, pos)
        case None =>
          for i <- cols.indices do ps.setNull(pos + i, cols(i))

  given Tuple2Codec[A, B](using
      aCodec: DbCodec[A],
      bCodec: DbCodec[B]
  ): DbCodec[(A, B)] with
    val cols: IArray[Int] = IArray.concat(aCodec.cols, bCodec.cols)
    def readSingle(rs: ResultSet, pos: Int): (A, B) = (
      aCodec.readSingle(rs, pos),
      bCodec.readSingle(rs, pos + aCodec.cols.length)
    )
    def writeSingle(tup: (A, B), ps: PreparedStatement, pos: Int): Unit =
      aCodec.writeSingle(tup._1, ps, pos)
      bCodec.writeSingle(tup._2, ps, pos + aCodec.cols.length)

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
    def writeSingle(tup: (A, B, C), ps: PreparedStatement, pos: Int): Unit =
      var i = pos
      aCodec.writeSingle(tup._1, ps, i)
      i += aCodec.cols.length
      bCodec.writeSingle(tup._2, ps, i)
      i += bCodec.cols.length
      cCodec.writeSingle(tup._3, ps, i)

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
    def writeSingle(tup: (A, B, C, D), ps: PreparedStatement, pos: Int): Unit =
      var i = pos
      aCodec.writeSingle(tup._1, ps, i)
      i += aCodec.cols.length
      bCodec.writeSingle(tup._2, ps, i)
      i += bCodec.cols.length
      cCodec.writeSingle(tup._3, ps, i)
      i += cCodec.cols.length
      dCodec.writeSingle(tup._4, ps, i)

  inline given derived[E: Mirror.Of]: DbCodec[E] =
    ${ dbCodecImpl[E] }

  private def dbCodecImpl[E: Type](using Quotes): Expr[DbCodec[E]] =
    import quotes.reflect.*
    Expr.summon[Mirror.Of[E]].get match
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
            def writeSingle(e: E, ps: PreparedStatement, pos: Int): Unit =
              ${
                productWriteSingle[E, mets]('{ e }, '{ ps }, '{ pos }, '{ 0 })
              }
          }
        }
      case '{
            $ms: Mirror.SumOf[E] {
              type MirroredElemTypes = mets
              type MirroredElemLabels = mels
              type MirroredLabel = mel
            }
          } =>
        val nameMapExpr = buildSqlNameMap[E, mels, mets]
        val melExpr = Expr(Type.valueOfConstant[mel].toString)
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
            def writeSingle(entity: E, ps: PreparedStatement, pos: Int): Unit =
              nameMap.find((_, v) => v == entity) match
                case Some((k, _)) => ps.setString(pos, k)
                case None =>
                  throw IllegalArgumentException(
                    entity.toString + " not convertible to " + $melExpr
                  )
          }
        }

  private def buildSqlNameMap[
      E: Type,
      Mels: Type,
      Mets: Type
  ](using q: Quotes): Expr[Seq[(String, E)]] =
    import q.reflect.*
    val tableAnnot = TypeRepr.of[Table]
    val defaultNameMapper: Expr[SqlNameMapper] =
      TypeRepr
        .of[E]
        .typeSymbol
        .annotations
        .find(term => term.tpe =:= tableAnnot) match
        case Some(term) =>
          val tableExpr = term.asExprOf[Table]
          '{ $tableExpr.nameMapper }
        case None =>
          '{ SqlNameMapper.SameCase }

    val sumValueExprs: Vector[Expr[E]] = sumValues[E, Mets]()
    val scalaNames = getScalaNames[Mels]()

    val sqlNameAnnot = TypeRepr.of[SqlName]
    val sqlNameExprs: Vector[Expr[(String, E)]] = scalaNames
      .zip(sumValueExprs)
      .map((scalaName, sumExpr) =>
        val symbol = TypeRepr.of[E].typeSymbol.fieldMember(scalaName.toString)
        symbol.annotations.find(term => term.tpe =:= sqlNameAnnot) match
          case Some(term) =>
            val sqlNameExpr: Expr[SqlName] = term.asExprOf[SqlName]
            '{ ($sqlNameExpr.name.toString, $sumExpr) }
          case None =>
            val scalaNameExpr = Expr(scalaName.toString)
            '{ ($defaultNameMapper.toColumnName($scalaNameExpr), $sumExpr) }
      )
    Expr.ofSeq(sqlNameExprs)

  private def getScalaNames[Mels: Type](res: Vector[String] = Vector.empty)(
      using Quotes
  ): Vector[String] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val melString = Type.valueOfConstant[mel & String].get
        getScalaNames[melTail](res :+ melString)
      case '[EmptyTuple] => res

  private def sumValues[E: Type, Mets: Type](
      res: Vector[Expr[E]] = Vector.empty
  )(using Quotes): Vector[Expr[E]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val expr = Expr.summon[Mirror.ProductOf[met & E]] match
          case Some(m) if isSingleton[met] => '{ $m.fromProduct(EmptyTuple) }
          case None =>
            report.error("Can only derive simple (non-adt) enums")
            '{ ??? }
        sumValues[E, metTail](res :+ expr)
      case '[EmptyTuple] => res

  private def isSingleton[T: Type](using Quotes): Boolean =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[T]] match
      case Some('{
            $mp: Mirror.ProductOf[T] {
              type MirroredElemTypes = mets
            }
          }) =>
        tupleArity[mets]() == 0
      case None => false

  private def tupleArity[T: Type](res: Int = 0)(using Quotes): Int =
    import quotes.reflect.*
    Type.of[T] match
      case '[x *: xs]    => tupleArity[xs](res + 1)
      case '[EmptyTuple] => res

  private def buildColsExpr[Mets: Type](
      res: Vector[Expr[IArray[Int]]] = Vector.empty
  )(using Quotes): Expr[IArray[Int]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val metCodec = Expr.summon[DbCodec[met]].get
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
                report.error(
                  "Could not find DbCodec or ClassTag for ${TypeRepr.of[met].show}"
                )
                '{ ??? }
      case '[EmptyTuple] =>
        '{
          val product = ${ Expr.ofTupleFromSeq(res) }
          $m.fromProduct(product)
        }

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
