package com.augustnagro.magnum

import java.sql.{Connection, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.deriving.Mirror
import scala.compiletime.{
  constValue,
  constValueTuple,
  erasedValue,
  error,
  summonInline
}
import scala.compiletime.ops.any.==
import scala.compiletime.ops.boolean.&&
import scala.reflect.ClassTag
import scala.quoted.*
import scala.util.{Failure, Success, Using}

sealed trait DbSchema[EC, E, ID] extends Selectable:
  def selectDynamic(scalaName: String): Any
  def all: IArray[DbSchemaName]
  def alias: String
  def alias(tableSqlAlias: String): this.type

  private[magnum] def tableWithAlias: String
  private[magnum] def count(using DbCon): Long
  private[magnum] def existsById(id: ID)(using DbCon): Boolean
  private[magnum] def findAll(using DbCon): Vector[E]
  private[magnum] def findAll(spec: Spec[E])(using DbCon): Vector[E]
  private[magnum] def findById(id: ID)(using DbCon): Option[E]
  private[magnum] def findAllById(ids: Iterable[ID])(using DbCon): Vector[E]
  private[magnum] def deleteById(id: ID)(using DbCon): Unit
  private[magnum] def truncate()(using DbCon): Unit
  private[magnum] def deleteAllById(ids: Iterable[ID])(using DbCon): Unit
  private[magnum] def insert(entityCreator: EC)(using DbCon): E
  private[magnum] def insertAll(entityCreators: Iterable[EC])(using
      DbCon
  ): Vector[E]
  private[magnum] def update(entity: E)(using DbCon): Unit
  private[magnum] def updateAll(entities: Iterable[E])(using DbCon): Unit

object DbSchema:
  transparent inline def apply[EC <: Product, E <: Product, ID](
      sqlNameMapper: SqlNameMapper = SameCase
  )(using
      ecMirror: Mirror.ProductOf[EC],
      eMirror: Mirror.ProductOf[E],
      dbEntity: DbReader[E],
      idCls: ClassTag[ID]
  ) = ${ dbSchemaImpl[EC, E, ID]('{ sqlNameMapper }) }

  // todo assert EC effective <: E
  private def dbSchemaImpl[EC: Type, E: Type, ID: Type](
      sqlNameMapper: Expr[SqlNameMapper]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[E]].get match
      case '{
            $m: Mirror.ProductOf[E] {
              type MirroredElemLabels = mels
              type MirroredLabel = tableName
            }
          } =>
        val tableName = Expr(Type.valueOfConstant[tableName].get.toString)
        val tableNameSql = '{ $sqlNameMapper.toTableName($tableName) }
        applyRefinements[EC, E, ID, mels, DbSchema[EC, E, ID]](
          sqlNameMapper,
          tableNameSql,
          Nil
        )

  private def applyRefinements[
      EC: Type,
      E: Type,
      ID: Type,
      EMels: Type,
      RES: Type
  ](
      sqlNameMapper: Expr[SqlNameMapper],
      tableNameSql: Expr[String],
      fieldNames: List[String]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    Type.of[EMels] match
      case '[mel *: melTail] =>
        val scalaFieldName = Type.valueOfConstant[mel].get.toString
        val fieldNameRefinement =
          Refinement(
            TypeRepr.of[RES],
            scalaFieldName,
            TypeRepr.of[DbSchemaName]
          )
        fieldNameRefinement.asType match
          case '[tpe] =>
            applyRefinements[EC, E, ID, melTail, tpe](
              sqlNameMapper,
              tableNameSql,
              scalaFieldName :: fieldNames
            )
      case '[EmptyTuple] =>
        buildDbSchema[EC, E, ID, RES](
          tableNameSql,
          Expr(fieldNames),
          ecFieldNames[EC],
          sqlNameMapper
        )

  private def ecFieldNames[EC: Type](using Quotes): Expr[List[String]] =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[EC]].get match
      case '{
            $m: Mirror.ProductOf[EC] {
              type MirroredElemLabels = mels
            }
          } =>
        ecFieldNamesImpl[mels](Nil)

  private def ecFieldNamesImpl[Mels: Type](
      res: List[String]
  )(using Quotes): Expr[List[String]] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val fieldName = Type.valueOfConstant[mel].get.toString
        ecFieldNamesImpl[melTail](fieldName :: res)
      case '[EmptyTuple] =>
        Expr(res)

  private def buildDbSchema[
      EC: Type,
      E: Type,
      ID: Type,
      RES: Type
  ](
      tableNameSql: Expr[String],
      fieldNames: Expr[List[String]],
      ecFieldNames: Expr[List[String]],
      sqlNameMapper: Expr[SqlNameMapper]
  )(using Quotes): Expr[Any] =
    val dbReaderExpr = Expr.summon[DbReader[E]].get
    val idClassTag = Expr.summon[ClassTag[ID]].get
    val eMirrorExpr = Expr.summon[Mirror.ProductOf[E]].get
    '{
      given dbReader: DbReader[E] = $dbReaderExpr
      given ClassTag[ID] = $idClassTag
      val eMirror = $eMirrorExpr
      val nameMapper: SqlNameMapper = $sqlNameMapper
      val tblNameSql: String = $tableNameSql
      val defaultAlias = ""

      val schemaNames: IArray[DbSchemaName] = IArray
        .from($fieldNames)
        .map(fn =>
          DbSchemaName(
            scalaName = fn,
            sqlName = nameMapper.toColumnName(fn),
            tableAlias = defaultAlias
          )
        )
        .reverse

      val ecInsertFields: IArray[String] =
        IArray.from($ecFieldNames).reverse.map(nameMapper.toColumnName)
      val ecInsertKeys = ecInsertFields.mkString("(", ", ", ")")
      val ecInsertQs =
        IArray.fill(ecInsertFields.size)("?").mkString("(", ", ", ")")

      val insertSql =
        s"insert into $tblNameSql $ecInsertKeys values $ecInsertQs"

      // todo make DbSchema a class with these parameters instead?
      class DbSchemaImpl(
          tableAlias: String,
          schemaNames: IArray[DbSchemaName],
          idName: DbSchemaName
      ) extends DbSchema[EC, E, ID]:

        def selectDynamic(scalaName: String): Any =
          schemaNames.find(_.scalaName == scalaName).get

        def all: IArray[DbSchemaName] = schemaNames

        def alias: String = tableAlias

        def alias(tableAlias: String): this.type =
          val newSchemaNames =
            schemaNames.map(sn => sn.copy(tableAlias = tableAlias))
          new DbSchemaImpl(
            tableAlias,
            newSchemaNames,
            newSchemaNames(0)
          ).asInstanceOf[this.type]

        def tableWithAlias: String =
          if tableAlias.isEmpty then tblNameSql
          else tblNameSql + " " + tableAlias

        def count(using con: DbCon): Long =
          sql"select count(*) from $this".run[Long].head

        def existsById(id: ID)(using DbCon): Boolean =
          sql"select 1 from $this where $idName = $id".run[Int].nonEmpty

        def findAll(using DbCon): Vector[E] =
          sql"select * from $this".run

        def findAll(spec: Spec[E])(using DbCon): Vector[E] = ???

        def findById(id: ID)(using DbCon): Option[E] =
          sql"select * from $this where $idName = $id".run[E].headOption

        def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
          sql"select * from $this where $idName = ANY(${ids.toArray})".run

        def deleteById(id: ID)(using DbCon): Unit =
          sql"delete from $this where $idName = $id".runUpdate

        def truncate()(using DbCon): Unit =
          sql"truncate table $this".runUpdate

        // todo use batch
        def deleteAllById(ids: Iterable[ID])(using DbCon): Unit =
          sql"delete from $this where $idName = ANY(${ids.toArray})".runUpdate

        def insert(entityCreator: EC)(using con: DbCon): E =
          val ecProduct = entityCreator.asInstanceOf[Product]
          val ecValues = ecProduct.productIterator.toVector

          Using.Manager(use =>
            val ps = use(
              con.connection
                .prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
            )
            setValues(ps, ecValues)
            ps.executeUpdate()
            val rs = use(ps.getGeneratedKeys)
            rs.next()
            val c = rs.getMetaData.getColumnCount
            println("c: " + c)
            for x <- 1 to c do println(s"$x = ${rs.getObject(x)}")
            println("xx: " + rs.getObject(2))
            println("yy: " + rs.getObject(2, classOf[OffsetDateTime]))
            ${ eFromInsert[E, EC]('{ rs }, '{ ecProduct }) }
          ) match
            case Success(res) => res
            case Failure(ex) => throw SqlException(ex, Sql(insertSql, ecValues))

        def insertAll(entityCreators: Iterable[EC])(using DbCon): Vector[E] =
          ???

        def update(entity: E)(using DbCon): Unit = ???

        def updateAll(entities: Iterable[E])(using DbCon): Unit = ???
      end DbSchemaImpl

      DbSchemaImpl(defaultAlias, schemaNames, schemaNames.head)
        .asInstanceOf[RES]
    }

  private def eFromInsert[E: Type, EC: Type](
      rs: Expr[ResultSet],
      ec: Expr[Product]
  )(using Quotes): Expr[E] =
    import quotes.reflect.*
    val eMirror = Expr.summon[Mirror.ProductOf[E]].get
    val ecMirror = Expr.summon[Mirror.ProductOf[EC]].get
    (eMirror, ecMirror) match
      case (
            '{
              $eM: Mirror.ProductOf[E] {
                type MirroredElemTypes = eMets
                type MirroredElemLabels = eMels
              }
            },
            '{
              $ecM: Mirror.ProductOf[EC] {
                type MirroredElemLabels = ecMels
              }
            }
          ) =>
        eFromInsertImpl[E, EC, eMets, eMels, ecMels](rs, ec, Vector.empty)

  private def eFromInsertImpl[
      E: Type,
      EC: Type,
      EMets: Type,
      EMels: Type,
      ECMels: Type
  ](
      rs: Expr[ResultSet],
      ec: Expr[Product],
      exprs: Vector[Expr[Any]],
      rsCol: Int = 1
  )(using Quotes): Expr[E] =
    import quotes.reflect.*
    (Type.of[EMets], Type.of[EMels]) match
      case ('[EmptyTuple], '[EmptyTuple]) =>
        val mirror = Expr.summon[Mirror.ProductOf[E]].get
        val productExpr = Expr.ofTupleFromSeq(exprs)
        '{ $mirror.fromProduct($productExpr) }

      case ('[eMet *: eMetTail], '[eMel *: eMelTail]) =>
        val eFieldName = Type.valueOfConstant[eMel].get.toString
        findEcIndex[ECMels](eFieldName) match
          case Some(ecIndex) =>
            val expr = '{ $ec.productElement(${ Expr(ecIndex) }) }
            eFromInsertImpl[E, EC, eMetTail, eMelTail, ECMels](
              rs,
              ec,
              exprs :+ expr,
              rsCol
            )
          case None =>
            val expr = fromRow[eMet](rs, Expr(rsCol))
            eFromInsertImpl[E, EC, eMetTail, eMelTail, ECMels](
              rs,
              ec,
              exprs :+ expr,
              rsCol + 1
            )

  private def findEcIndex[ECMels: Type](eFieldName: String, i: Int = 0)(using
      Quotes
  ): Option[Int] =
    Type.of[ECMels] match
      case '[EmptyTuple] =>
        None
      case '[ecMel *: ecMelTail] =>
        val ecFieldName = Type.valueOfConstant[ecMel].get.toString
        if ecFieldName == eFieldName then Some(i)
        else findEcIndex[ecMelTail](eFieldName, i + 1)

  private def fromRow[Met: Type](rs: Expr[ResultSet], col: Expr[Int])(using
      Quotes
  ): Expr[Any] =
    Type.of[Met] match
      case '[String]                => '{ $rs.getString($col) }
      case '[Boolean]               => '{ $rs.getBoolean($col) }
      case '[Byte]                  => '{ $rs.getByte($col) }
      case '[Short]                 => '{ $rs.getShort($col) }
      case '[Int]                   => '{ $rs.getInt($col) }
      case '[Long]                  => '{ $rs.getLong($col) }
      case '[Float]                 => '{ $rs.getFloat($col) }
      case '[Double]                => '{ $rs.getDouble($col) }
      case '[Array[Byte]]           => '{ $rs.getBytes($col) }
      case '[java.sql.Date]         => '{ $rs.getDate($col) }
      case '[java.sql.Time]         => '{ $rs.getTime($col) }
      case '[java.sql.Timestamp]    => '{ $rs.getTimestamp($col) }
      case '[java.sql.Ref]          => '{ $rs.getRef($col) }
      case '[java.sql.Blob]         => '{ $rs.getBlob($col) }
      case '[java.sql.Clob]         => '{ $rs.getClob($col) }
      case '[java.net.URL]          => '{ $rs.getURL($col) }
      case '[java.sql.RowId]        => '{ $rs.getRowId($col) }
      case '[java.sql.NClob]        => '{ $rs.getNClob($col) }
      case '[java.sql.SQLXML]       => '{ $rs.getSQLXML($col) }
      case '[scala.math.BigDecimal] => '{ BigDecimal($rs.getBigDecimal($col)) }
      case '[scala.math.BigInt] =>
        '{ BigInt($rs.getObject($col, classOf[java.math.BigInteger])) }
      case '[Option[t]] => '{ Option(${ fromRow[t](rs, col) }) }
      case _ =>
        val cls = Expr.summon[ClassTag[Met]].get
        '{ $rs.getObject($col, $cls.runtimeClass) }
