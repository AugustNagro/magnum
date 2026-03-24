package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object SqliteDbType extends DbType:

  private val specImpl = new SpecImpl:
    override def offsetLimitSql(
        offset: Option[Long],
        limit: Option[Int]
    ): Option[String] =
      (offset, limit) match
        case (Some(o), Some(l)) => Some(s"LIMIT $o, $l")
        case (Some(o), None)    => Some(s"LIMIT $o, ${Long.MaxValue}")
        case (None, Some(l))    => Some(s"LIMIT $l")
        case (None, None)       => None

  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      eElemNames: Seq[String],
      eElemNamesSql: Seq[String],
      eElemCodecs: Seq[DbCodec[?]],
      ecElemNames: Seq[String],
      ecElemNamesSql: Seq[String],
      idIndexes: List[Int]
  )(using
      eCodec: DbCodec[E],
      ecCodec: DbCodec[EC],
      idCodec: DbCodec[ID],
      eClassTag: ClassTag[E],
      ecClassTag: ClassTag[EC],
      idClassTag: ClassTag[ID]
  ): RepoDefaults[EC, E, ID] =
    val idNames = idIndexes.map(eElemNamesSql)
    val idKeys = idNames.mkString("(", ", ", ")")
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")

    val eElemNamesAndCodecs = eElemNamesSql.lazyZip(eElemCodecs)
    val (idNamesAndCodecs, ecNamesAndCodecs) =
      eElemNamesAndCodecs.partition((sqlName, _) => idNames.contains(sqlName))

    val idFilter = idKeys + " = " + idCodec.queryRepr

    val updateKeys: String = ecNamesAndCodecs
      .map((sqlName, codec) => sqlName + " = " + codec.queryRepr)
      .mkString(", ")

    val updateCodecs =
      (ecNamesAndCodecs.map(_._2) ++ idNamesAndCodecs.map(_._2))
        .toSeq
        .asInstanceOf[Seq[DbCodec[Any]]]
    val insertGenKeys = eElemNamesSql.toArray

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql, Vector.empty, FragWriter.empty).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idFilter"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql, Vector.empty, FragWriter.empty).query[E]
    val findByIdSql =
      s"SELECT * FROM $tableNameSql WHERE $idFilter"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idFilter"
    val truncateSql = s"DELETE FROM $tableNameSql"
    val truncateUpdate =
      Frag(truncateSql, Vector.empty, FragWriter.empty).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"
    val updateSql =
      s"UPDATE $tableNameSql SET $updateKeys WHERE $idFilter"

    def idWriter(id: ID): FragWriter = (ps, pos) =>
      idCodec.writeSingle(id, ps, pos)
      pos + idCodec.cols.length

    def entityId(entity: E): ID =
      val idElems = idIndexes.map(entity.asInstanceOf[Product].productElement)
      idElems match
        case head :: Nil => head.asInstanceOf[ID]
        case _ => idClassTag.runtimeClass.getDeclaredConstructors().head.newInstance(idElems*).asInstanceOf[ID]

    new RepoDefaults[EC, E, ID]:
      def count(using con: DbCon): Long = countQuery.run().head

      def existsById(id: ID)(using DbCon): Boolean =
        Frag(existsByIdSql, IArray(id), idWriter(id))
          .query[Int]
          .run()
          .nonEmpty

      def findAll(using DbCon): Vector[E] = findAllQuery.run()

      def findAll(spec: Spec[E])(using DbCon): Vector[E] =
        specImpl.findAll(spec, tableNameSql)

      def findById(id: ID)(using DbCon): Option[E] =
        Frag(findByIdSql, IArray(id), idWriter(id))
          .query[E]
          .run()
          .headOption

      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
        throw UnsupportedOperationException(
          "Sqlite does not support 'ANY' keyword, and does not support long IN parameter lists. Use findById in a loop instead."
        )

      def delete(entity: E)(using DbCon): Unit =
        deleteById(
          entityId(entity)
        )

      def deleteById(id: ID)(using DbCon): Unit =
        Frag(deleteByIdSql, IArray(id), idWriter(id)).update
          .run()

      def truncate()(using DbCon): Unit =
        truncateUpdate.run()

      def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult =
        deleteAllById(
          entities.map(entityId)
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

      // https://github.com/AugustNagro/magnum/issues/87#issuecomment-2591823574
      def insertReturning(entityCreator: EC)(using con: DbCon): E =
        throw UnsupportedOperationException()

      // https://github.com/AugustNagro/magnum/issues/87#issuecomment-2591823574
      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        throw UnsupportedOperationException()

      def update(entity: E)(using con: DbCon): Unit =
        handleQuery(updateSql, entity):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            val entityValues: Vector[Any] = entity
              .asInstanceOf[Product]
              .productIterator
              .toVector
            // put ID at the end
            val (idValues, ecValues) = entityValues.zipWithIndex
              .partition((_, index) => idIndexes.contains(index))
            val updateValues = ecValues.map(_._1) ++ idValues.map(_._1)

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
              val (idValues, ecValues) = entityValues.zipWithIndex
                .partition((_, index) => idIndexes.contains(index))
              val updateValues = ecValues.map(_._1) ++ idValues.map(_._1)

              var pos = 1
              for (field, codec) <- updateValues.lazyZip(updateCodecs) do
                codec.writeSingle(field, ps, pos)
                pos += codec.cols.length
              ps.addBatch()

            timed(batchUpdateResult(ps.executeBatch()))
    end new
  end buildRepoDefaults
end SqliteDbType
