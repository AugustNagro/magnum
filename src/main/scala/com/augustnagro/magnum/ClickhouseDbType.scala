package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using, boundary}

object ClickhouseDbType /*extends DbType:
  def buildDbSchema[EC, E, ID, RES](
      tableNameSql: String,
      fieldNames: List[String],
      ecFieldNames: List[String],
      sqlNameMapper: SqlNameMapper,
      idIndex: Int
  )(using
    dbReader: DbCodec[E],
    ecClassTag: ClassTag[EC],
    eClassTag: ClassTag[E],
    idClassTag: ClassTag[ID],
    eMirror: Mirror.ProductOf[E]
  ): RES =
    require(
      eClassTag.runtimeClass == ecClassTag.runtimeClass,
      "ClickHouse does not support generated keys, so EC must equal E"
    )
    val schemaNames: IArray[DbSchemaName] = IArray
      .from(fieldNames)
      .map(fn =>
        DbSchemaName(
          scalaName = fn,
          sqlName = sqlNameMapper.toColumnName(fn),
          tableAlias = DbSchema.DefaultAlias
        )
      )
    val idName = schemaNames(idIndex).sqlName

    val ecInsertFields: IArray[String] =
      IArray.from(ecFieldNames).map(sqlNameMapper.toColumnName)
    val ecInsertKeys: String = ecInsertFields.mkString("(", ", ", ")")
    val ecInsertQs =
      IArray.fill(ecInsertFields.size)("?").mkString("(", ", ", ")")

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val existsByIdSql = s"SELECT 1 FROM $tableNameSql WHERE $idName = ?"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findByIdSql = s"SELECT * FROM $tableNameSql WHERE $idName = ?"
    val deleteByIdSql = s"DELETE FROM $tableNameSql WHERE $idName = ?"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES $ecInsertQs"

    class ClickHouseSchema(
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
        new ClickHouseSchema(
          tableAlias,
          newSchemaNames
        ).asInstanceOf[this.type]

      def tableWithAlias: String =
        if tableAlias.isEmpty then tableNameSql
        else tableNameSql + " " + tableAlias

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
        throw UnsupportedOperationException()

      def delete(entity: E)(using DbCon): Boolean =
        deleteById(
          entity
            .asInstanceOf[Product]
            .productElement(idIndex)
            .asInstanceOf[ID]
        )

      def deleteById(id: ID)(using DbCon): Boolean =
        Sql(deleteByIdSql, Vector(id)).runUpdate > 0

      def truncate()(using DbCon): Int =
        Sql(truncateSql, Vector.empty).runUpdate

      def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult =
        deleteAllById(
          entities.map(e =>
            e.asInstanceOf[Product].productElement(idIndex).asInstanceOf[ID]
          )
        )

      def deleteAllById(ids: Iterable[ID])(using
          con: DbCon
      ): BatchUpdateResult =
        logSql(deleteByIdSql, ids)
        Using(con.connection.prepareStatement(deleteByIdSql))(ps =>
          for id <- ids do
            ps.setObject(1, id)
            ps.addBatch()
          batchUpdateResult(ps.executeBatch())
        ) match
          case Success(res) => res
          case Failure(t) =>
            throw SqlException(deleteByIdSql, ids, t)

      def insert(entityCreator: EC)(using con: DbCon): Unit =
        val insertValues = ProductIterable(entityCreator.asInstanceOf[Product])
        logSql(insertSql, insertValues)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          setValues(ps, insertValues)
          ps.executeUpdate()
        ) match
          case Success(_) => ()
          case Failure(ex) =>
            throw SqlException(insertSql, insertValues, ex)

      def insertAll(entityCreators: Iterable[EC])(using con: DbCon): Unit =
        val batchInsertValues = entityCreators
          .map(ec => ProductIterable(ec.asInstanceOf[Product]))
        logSql(insertSql, batchInsertValues)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          for insertValues <- batchInsertValues do
            setValues(ps, insertValues)
            ps.addBatch()
          batchUpdateResult(ps.executeBatch())
        ) match
          case Success(_) => ()
          case Failure(ex) =>
            throw SqlException(insertSql, batchInsertValues, ex)

      def insertReturning(entityCreator: EC)(using con: DbCon): E =
        val insertValues = ProductIterable(entityCreator.asInstanceOf[Product])
        logSql(insertSql, insertValues)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          setValues(ps, insertValues)
          ps.executeUpdate()
          entityCreator.asInstanceOf[E]
        ) match
          case Success(res) => res
          case Failure(ex) =>
            throw SqlException(insertSql, insertValues, ex)

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        val batchInsertValues = entityCreators
          .map(ec => ProductIterable(ec.asInstanceOf[Product]))
        logSql(insertSql, batchInsertValues)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          for insertValues <- batchInsertValues do
            setValues(ps, insertValues)
            ps.addBatch()
          ps.executeBatch()
          entityCreators.toVector.asInstanceOf[Vector[E]]
        ) match
          case Success(res) => res
          case Failure(ex) =>
            throw SqlException(insertSql, batchInsertValues, ex)

      def update(entity: E)(using DbCon): Boolean =
        throw UnsupportedOperationException()

      def updateAll(entities: Iterable[E])(using con: DbCon): BatchUpdateResult =
        throw UnsupportedOperationException()

    end ClickHouseSchema
    ClickHouseSchema(DbSchema.DefaultAlias, schemaNames).asInstanceOf[RES]

*/