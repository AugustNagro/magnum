package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object SqliteDbType /*extends DbType:
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

    val updateKeys: String = schemaNames
      .map(sn => sn.sqlName + " = ?")
      .patch(idIndex, IArray.empty[String], 1)
      .mkString(", ")

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val existsByIdSql = s"SELECT 1 FROM $tableNameSql WHERE $idName = ?"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findByIdSql = s"SELECT * FROM $tableNameSql WHERE $idName = ?"
    val deleteByIdSql = s"DELETE FROM $tableNameSql WHERE $idName = ?"
    val truncateSql = s"DELETE FROM $tableNameSql"
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES $ecInsertQs"
    val updateSql = s"UPDATE $tableNameSql SET $updateKeys WHERE $idName = ?"

    class SqliteSchema(
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
        new SqliteSchema(
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
        throw UnsupportedOperationException(
          "Sqlite does not support 'ANY' keyword, and does not support long IN parameter lists. Use findById in a loop instead."
        )

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
          logSql(deleteByIdSql, Vector.empty)
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
          val insertValues =
            entityCreator.asInstanceOf[Product].productIterator.toVector
          logSql(insertSql, insertValues)
          val ps =
            use(
              con.connection
                .prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
            )
          setValues(ps, insertValues)
          ps.executeUpdate()
          val rs = use(ps.getGeneratedKeys)
          rs.next()
          // unsafe cast, but next line will fail if wrong.
          val id = rs.getObject(1).asInstanceOf[ID]
          // unfortunately, sqlite only will return the primary key.
          // it doesn't return default columns, and adding other columns to
          // the insertGenKeys array doesn't change this behavior. So we need
          // to query by ID after inserting.
          findById(id).get
        ) match
          case Success(res) => res
          case Failure(ex) =>
            throw SqlException(ex, Sql(insertSql, Vector.empty))

      def insertAll(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        Using.Manager(use =>
          logSql(insertSql, Vector.empty)
          val ps = use(
            con.connection
              .prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
          )
          for ec <- entityCreators do
            setValues(ps, ec.asInstanceOf[Product].productIterator.toVector)
            ps.addBatch()
          ps.executeBatch()
          val rs = use(ps.getGeneratedKeys)
          val resBuilder = Vector.newBuilder[E]
          while rs.next() do
            val id = rs.getObject(1).asInstanceOf[ID]
            resBuilder += findById(id).get
          resBuilder.result()
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
          logSql(updateSql, Vector.empty)
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
    end SqliteSchema
    SqliteSchema(DbSchema.DefaultAlias, schemaNames).asInstanceOf[RES]

*/