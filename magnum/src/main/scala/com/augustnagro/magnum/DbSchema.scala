package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
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
  def selectDynamic(scalaName: String): DbSchemaName
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
  private[magnum] def delete(entity: E)(using DbCon): Unit
  private[magnum] def deleteById(id: ID)(using DbCon): Unit
  private[magnum] def truncate()(using DbCon): Unit
  private[magnum] def deleteAll(entities: Iterable[E])(using DbCon): Unit
  private[magnum] def deleteAllById(ids: Iterable[ID])(using DbCon): Unit
  private[magnum] def insert(entityCreator: EC)(using DbCon): E
  private[magnum] def insertAll(entityCreators: Iterable[EC])(using
      DbCon
  ): Vector[E]
  private[magnum] def update(entity: E)(using DbCon): Unit
  private[magnum] def updateAll(entities: Iterable[E])(using DbCon): Unit

object DbSchema:
  transparent inline def apply[EC <: Product, E <: Product, ID](
      sqlNameMapper: SqlNameMapper = SameCase,
      // todo
      dbType: DbType = DbType.Postgres
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
          Expr(fieldNames.reverse),
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
        Expr(res.reverse)

  private def idAnnotIndex[E: Type](using Quotes): Expr[Int] =
    import quotes.reflect.*
    val idAnnot = TypeRepr.of[Id]
    val index = TypeRepr
      .of[E]
      .typeSymbol
      .caseFields
      .indexWhere(sym =>
        sym.annotations.exists(term => term.tpe =:= idAnnot)
      ) match
      case -1 => 0
      case x  => x
    Expr(index)

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

      val idIndex = ${ idAnnotIndex[E] }
      val idName = schemaNames(idIndex).sqlName

      val ecInsertFields: IArray[String] =
        IArray.from($ecFieldNames).map(nameMapper.toColumnName)
      val ecInsertKeys = ecInsertFields.mkString("(", ", ", ")")
      val ecInsertQs =
        IArray.fill(ecInsertFields.size)("?").mkString("(", ", ", ")")

      val updateKeys: String = schemaNames
        .map(sn => sn.sqlName + " = ?")
        .patch(idIndex, IArray.empty, 1)
        .mkString(", ")

      val countSql = s"SELECT count(*) FROM $tblNameSql"
      val existsByIdSql = s"SELECT 1 FROM $tblNameSql WHERE $idName = ?"
      val findAllSql = s"SELECT * FROM $tblNameSql"
      val findByIdSql = s"SELECT * FROM $tblNameSql WHERE $idName = ?"
      val findAllByIdSql = s"SELECT * FROM $tblNameSql WHERE $idName = ANY(?)"
      val deleteByIdSql = s"DELETE FROM $tblNameSql WHERE $idName = ?"
      val truncateSql = s"TRUNCATE TABLE $tblNameSql"
      val insertSql =
        s"INSERT INTO $tblNameSql $ecInsertKeys VALUES $ecInsertQs"
      val updateSql = s"UPDATE $tblNameSql SET $updateKeys WHERE $idName = ?"

      // todo make DbSchema a class with these parameters instead?
      class DbSchemaImpl(
          tableAlias: String,
          schemaNames: IArray[DbSchemaName]
      ) extends DbSchema[EC, E, ID]:

        def selectDynamic(scalaName: String): DbSchemaName =
          schemaNames.find(_.scalaName == scalaName).get

        def all: IArray[DbSchemaName] = schemaNames

        def alias: String = tableAlias

        def alias(tableAlias: String): this.type =
          val newSchemaNames =
            schemaNames.map(sn => sn.copy(tableAlias = tableAlias))
          new DbSchemaImpl(
            tableAlias,
            newSchemaNames
          ).asInstanceOf[this.type]

        def tableWithAlias: String =
          if tableAlias.isEmpty then tblNameSql
          else tblNameSql + " " + tableAlias

        def count(using con: DbCon): Long =
          Sql(countSql, Vector.empty).run[Long].head

        def existsById(id: ID)(using DbCon): Boolean =
          Sql(existsByIdSql, Vector(id)).run[Int].nonEmpty

        def findAll(using DbCon): Vector[E] =
          Sql(findAllSql, Vector.empty).run

        def findAll(spec: Spec[E])(using DbCon): Vector[E] =
          spec.build.run

        def findById(id: ID)(using DbCon): Option[E] =
          Sql(findByIdSql, Vector(id)).run[E].headOption

        def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
          Sql(findAllByIdSql, Vector(ids.toArray)).run

        def delete(entity: E)(using DbCon): Unit =
          deleteById(
            entity
              .asInstanceOf[Product]
              .productElement(idIndex)
              .asInstanceOf[ID]
          )

        def deleteById(id: ID)(using DbCon): Unit =
          Sql(deleteByIdSql, Vector(id)).runUpdate

        def truncate()(using DbCon): Unit =
          Sql(truncateSql, Vector.empty).runUpdate

        def deleteAll(entities: Iterable[E])(using DbCon): Unit =
          deleteAllById(
            entities.map(e =>
              e.asInstanceOf[Product].productElement(idIndex).asInstanceOf[ID]
            )
          )

        def deleteAllById(ids: Iterable[ID])(using con: DbCon): Unit =
          Using.Manager(use =>
            val ps = use(con.connection.prepareStatement(deleteByIdSql))
            for id <- ids do
              ps.setObject(1, id)
              ps.addBatch()
            ps.executeBatch()
          ) match
            case Success(_) => ()
            case Failure(t) =>
              throw SqlException(t, Sql(deleteByIdSql, Vector.empty))

        def insert(entityCreator: EC)(using con: DbCon): E =
          Using.Manager(use =>
            val ps = use(
              con.connection
                .prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
            )
            setValues(
              ps,
              entityCreator.asInstanceOf[Product].productIterator.toVector
            )
            ps.executeUpdate()
            val rs = use(ps.getGeneratedKeys)
            rs.next()
            dbReader.buildSingle(rs)
          ) match
            case Success(res) => res
            case Failure(ex) =>
              throw SqlException(ex, Sql(insertSql, Vector.empty))

        def insertAll(
            entityCreators: Iterable[EC]
        )(using con: DbCon): Vector[E] =
          Using.Manager(use =>
            val ps = use(
              con.connection
                .prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
            )
            for ec <- entityCreators do
              setValues(ps, ec.asInstanceOf[Product].productIterator.toVector)
              ps.addBatch()
            ps.executeBatch()
            val rs = use(ps.getGeneratedKeys)
            dbReader.build(rs)
          ) match
            case Success(res) => res
            case Failure(t) =>
              throw SqlException(t, Sql(insertSql, Vector.empty))

        def update(entity: E)(using DbCon): Unit =
          val entityValues: Vector[Any] = entity
            .asInstanceOf[Product]
            .productIterator
            .toVector
          // put ID at the end
          val updateValues =
            entityValues
              .patch(idIndex, Vector.empty, 1)
              .appended(entityValues(idIndex))
          Sql(updateSql, updateValues).runUpdate

        def updateAll(entities: Iterable[E])(using con: DbCon): Unit =
          Using.Manager(use =>
            val ps = use(con.connection.prepareStatement(updateSql))
            for entity <- entities do
              val entityValues: Vector[Any] = entity
                .asInstanceOf[Product]
                .productIterator
                .toVector
              // put ID at the end
              val updateValues = entityValues
                .patch(idIndex, Vector.empty, 1)
                .appended(entityValues(idIndex))

              setValues(ps, updateValues)
              ps.addBatch()

            ps.executeBatch()
          ) match
            case Success(_) => ()
            case Failure(t) =>
              throw SqlException(t, Sql(updateSql, Vector.empty))
      end DbSchemaImpl

      DbSchemaImpl(defaultAlias, schemaNames).asInstanceOf[RES]
    }

private[magnum] def pgDbSchema[EC, E, ID, RES](
    dbReaderExpr: Expr[DbReader[E]],
    idClassTag: Expr[ClassTag[ID]],
    eMirrorExpr: Expr[Mirror.ProductOf[E]]
): Expr[RES] = ???

private[magnum] def mySqlDbSchema[EC, E, ID, RES](
    dbReaderExpr: Expr[DbReader[E]],
    idClassTag: Expr[ClassTag[ID]],
    eMirrorExpr: Expr[Mirror.ProductOf[E]]
): Expr[RES] = ???
