package com.augustnagro.magnum.dbtype

import com.augustnagro.magnum.batch.BatchUpdateResult
import com.augustnagro.magnum.builders.{DbCon, Frag, FragWriter}
import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.DbType
import com.augustnagro.magnum.{batchUpdateResult, handleQuery, timed}
import com.augustnagro.magnum.repo.RepoDefaults
import com.augustnagro.magnum.spec.{Spec, SpecImpl}

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object H2DbType extends DbType:

  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      eElemNames: Seq[String],
      eElemNamesSql: Seq[String],
      eElemCodecs: Seq[DbCodec[?]],
      ecElemNames: Seq[String],
      ecElemNamesSql: Seq[String],
      idIndex: Int
  )(using
      eCodec: DbCodec[E],
      ecCodec: DbCodec[EC],
      idCodec: DbCodec[ID],
      eClassTag: ClassTag[E],
      ecClassTag: ClassTag[EC],
      idClassTag: ClassTag[ID]
  ): RepoDefaults[EC, E, ID] =
    val idName = eElemNamesSql(idIndex)
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")

    val updateKeys: String = eElemNamesSql
      .lazyZip(eElemCodecs)
      .map((sqlName, codec) => sqlName + " = " + codec.queryRepr)
      .patch(idIndex, Seq.empty, 1)
      .mkString(", ")

    val updateCodecs = eElemCodecs
      .patch(idIndex, Seq.empty, 1)
      .appended(idCodec)
      .asInstanceOf[Seq[DbCodec[Any]]]

    val insertGenKeys: Array[String] = Array.from(eElemNamesSql)

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql, Vector.empty, FragWriter.empty).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql, Vector.empty, FragWriter.empty).query[E]
    val findByIdSql =
      s"SELECT * FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllByIdSql = s"SELECT * FROM $tableNameSql WHERE $idName = ANY(?)"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val truncateUpdate =
      Frag(truncateSql, Vector.empty, FragWriter.empty).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"
    val updateSql =
      s"UPDATE $tableNameSql SET $updateKeys WHERE $idName = ${idCodec.queryRepr}"

    val compositeId = idCodec.cols.distinct.size != 1
    val idFirstTypeName = JDBCType.valueOf(idCodec.cols.head).getName

    def idWriter(id: ID): FragWriter = (ps, pos) =>
      idCodec.writeSingle(id, ps, pos)
      pos + idCodec.cols.length

    new RepoDefaults[EC, E, ID]:
      def count(using con: DbCon): Long = countQuery.run().head

      def existsById(id: ID)(using DbCon): Boolean =
        Frag(existsByIdSql, IArray(id), idWriter(id))
          .query[Int]
          .run()
          .nonEmpty

      def findAll(using DbCon): Vector[E] = findAllQuery.run()

      def findAll(spec: Spec[E])(using DbCon): Vector[E] =
        SpecImpl.Default.findAll(spec, tableNameSql)

      def findById(id: ID)(using DbCon): Option[E] =
        Frag(findByIdSql, IArray(id), idWriter(id))
          .query[E]
          .run()
          .headOption

      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
        if compositeId then
          throw UnsupportedOperationException(
            "Composite ids unsupported for findAllById."
          )
        val idsArray = Array.from[Any](ids)
        Frag(
          findAllByIdSql,
          IArray(idsArray),
          (ps, pos) =>
            val sqlArray =
              ps.getConnection.createArrayOf(idFirstTypeName, idsArray)
            ps.setArray(pos, sqlArray)
            pos + 1
        ).query[E].run()
//        // h2 doesn't support setObject(..) with primitive arrays,
//        // so we need to convert to Array[Object]
//        val builder = Array.newBuilder[Object]
//        if ids.knownSize > -1 then builder.sizeHint(ids.knownSize)
//        for id <- ids do builder += id.asInstanceOf[Object]
//        Sql(findAllByIdSql, Vector(builder.result())).run

      def delete(entity: E)(using DbCon): Unit =
        deleteById(
          entity
            .asInstanceOf[Product]
            .productElement(idIndex)
            .asInstanceOf[ID]
        )

      def deleteById(id: ID)(using DbCon): Unit =
        Frag(deleteByIdSql, IArray(id), idWriter(id)).update.run()

      def truncate()(using DbCon): Unit = truncateUpdate.run()

      def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult =
        deleteAllById(
          entities.map(e =>
            e.asInstanceOf[Product].productElement(idIndex).asInstanceOf[ID]
          )
        )

      def deleteAllById(ids: Iterable[ID])(using
          con: DbCon
      ): BatchUpdateResult =
        handleQuery(deleteByIdSql, ids):
          Using(con.connection.prepareStatement(deleteByIdSql)): ps =>
            idCodec.write(ids, ps)
            timed(batchUpdateResult(ps.executeBatch()))

      def insert(entityCreator: EC)(using con: DbCon): Unit =
        handleQuery(insertSql, entityCreator):
          Using(con.connection.prepareStatement(insertSql)): ps =>
            ecCodec.writeSingle(entityCreator, ps)
            timed(ps.executeUpdate())

      def insertAll(entityCreators: Iterable[EC])(using con: DbCon): Unit =
        handleQuery(insertSql, entityCreators):
          Using(con.connection.prepareStatement(insertSql)): ps =>
            ecCodec.write(entityCreators, ps)
            timed(batchUpdateResult(ps.executeBatch()))

      def insertReturning(entityCreator: EC)(using con: DbCon): E =
        handleQuery(insertSql, entityCreator):
          Using.Manager: use =>
            val ps =
              use(con.connection.prepareStatement(insertSql, insertGenKeys))
            ecCodec.writeSingle(entityCreator, ps)
            timed:
              ps.executeUpdate()
              val rs = use(ps.getGeneratedKeys)
              rs.next()
              eCodec.readSingle(rs)

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        handleQuery(insertSql, entityCreators):
          Using.Manager: use =>
            val ps =
              use(con.connection.prepareStatement(insertSql, insertGenKeys))
            ecCodec.write(entityCreators, ps)
            timed:
              batchUpdateResult(ps.executeBatch())
              val rs = use(ps.getGeneratedKeys)
              eCodec.read(rs)

      def update(entity: E)(using con: DbCon): Unit =
        handleQuery(updateSql, entity):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            val entityValues: Vector[Any] = entity
              .asInstanceOf[Product]
              .productIterator
              .toVector
            // put ID at the end
            val updateValues = entityValues
              .patch(idIndex, Vector.empty, 1)
              .appended(entityValues(idIndex))

            var pos = 1
            for (field, codec) <- updateValues.lazyZip(updateCodecs) do
              codec.writeSingle(field, ps, pos)
              pos += codec.cols.length
            timed(ps.executeUpdate())

      def updateAll(entities: Iterable[E])(using
          con: DbCon
      ): BatchUpdateResult =
        handleQuery(updateSql, entities):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            for entity <- entities do
              val entityValues: Vector[Any] = entity
                .asInstanceOf[Product]
                .productIterator
                .toVector
              // put ID at the end
              val updateValues = entityValues
                .patch(idIndex, Vector.empty, 1)
                .appended(entityValues(idIndex))

              var pos = 1
              for (field, codec) <- updateValues.lazyZip(updateCodecs) do
                codec.writeSingle(field, ps, pos)
                pos += codec.cols.length
              ps.addBatch()

            timed(batchUpdateResult(ps.executeBatch()))

    end new
  end buildRepoDefaults
end H2DbType
