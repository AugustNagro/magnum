package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
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

object H2DbType extends DbType:
  def build[EC, E, ID, RES](
      tableNameSql: String,
      fieldNames: List[String],
      ecFieldNames: List[String],
      sqlNameMapper: SqlNameMapper,
      idIndex: Int
  )(using
      dbReader: DbReader[E],
      idClassTag: ClassTag[ID],
      eMirror: Mirror.ProductOf[E]
  ): RES =
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
    val ecInsertQs: String =
      IArray.fill(ecInsertFields.size)("?").mkString("(", ", ", ")")
    val insertGenKeys: Array[String] = Array.from(schemaNames).map(_.sqlName)

    val updateKeys: String = schemaNames
      .map(sn => sn.sqlName + " = ?")
      .patch(idIndex, IArray.empty[String], 1)
      .mkString(", ")

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val existsByIdSql = s"SELECT 1 FROM $tableNameSql WHERE $idName = ?"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findByIdSql = s"SELECT * FROM $tableNameSql WHERE $idName = ?"
    val findAllByIdSql = s"SELECT * FROM $tableNameSql WHERE $idName = ANY(?)"
    val deleteByIdSql = s"DELETE FROM $tableNameSql WHERE $idName = ?"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES $ecInsertQs"
    val updateSql = s"UPDATE $tableNameSql SET $updateKeys WHERE $idName = ?"

    class H2Schema(
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
        new H2Schema(
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
        // h2 doesn't support setObject(..) with primitive arrays,
        // so we need to convert to Array[Object]
        val builder = Array.newBuilder[Object]
        if ids.knownSize > -1 then builder.sizeHint(ids.knownSize)
        for id <- ids do builder += id.asInstanceOf[Object]
        Sql(findAllByIdSql, Vector(builder.result())).run

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
          val ps =
            use(con.connection.prepareStatement(insertSql, insertGenKeys))
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
          val ps =
            use(con.connection.prepareStatement(insertSql, insertGenKeys))
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
    end H2Schema
    H2Schema(DbSchema.DefaultAlias, schemaNames).asInstanceOf[RES]
