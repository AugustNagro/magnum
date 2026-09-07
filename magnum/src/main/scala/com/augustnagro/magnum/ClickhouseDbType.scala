package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import java.util.StringJoiner
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using, boundary}

object ClickhouseDbType extends DbType:
  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      eElemNames: Seq[String],
      eElemNamesSql: Seq[String],
      eElemCodecs: Seq[DbCodec[?]],
      ecElemNames: Seq[String],
      ecElemNamesSql: Seq[String],
      idIndices: Seq[Int],
      idFromProduct: Seq[Any] => ID
  )(using
      eCodec: DbCodec[E],
      ecCodec: DbCodec[EC],
      idCodec: DbCodec[ID],
      eClassTag: ClassTag[E],
      ecClassTag: ClassTag[EC],
      idClassTag: ClassTag[ID]
  ): RepoDefaults[EC, E, ID] =
    require(
      eClassTag.runtimeClass == ecClassTag.runtimeClass,
      "ClickHouse does not support generated keys, so EC must equal E"
    )
    val idNames = idIndices.map(eElemNamesSql)
    val hasId = idIndices.nonEmpty
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")
    val idCodecs =
      if hasId then idIndices.map(eElemCodecs).toVector
      else Vector(idCodec)

    val idWhereClause = idNames match
      case Seq() => "1 = 0"
      case _ =>
        idNames
          .zip(idCodecs)
          .map((name, codec) => name + " = " + codec.queryRepr)
          .mkString(" AND ")

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql, Vector.empty, FragWriter.empty).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idWhereClause"
    val findAllSql = s"SELECT $selectKeys FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql, Vector.empty, FragWriter.empty).query[E]
    val findByIdSql =
      s"SELECT $selectKeys FROM $tableNameSql WHERE $idWhereClause"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idWhereClause"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val truncateUpdate =
      Frag(truncateSql, Vector.empty, FragWriter.empty).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"

    def idWriter(id: ID): FragWriter = (ps, pos) =>
      idCodec.writeSingle(id, ps, pos)
      pos + idCodec.cols.length

    val existsByIdImpl: (ID, DbCon) => Boolean =
      if hasId then
        (id, con) =>
          Frag(existsByIdSql, IArray(id), idWriter(id))
            .query[Int]
            .run()(using con)
            .nonEmpty
      else (_, _) => false

    val findByIdImpl: (ID, DbCon) => Option[E] =
      if hasId then
        (id, con) =>
          Frag(findByIdSql, IArray(id), idWriter(id))
            .query[E]
            .run()(using con)
            .headOption
      else (_, _) => None

    val deleteByIdImpl: (ID, DbCon) => Unit =
      if hasId then
        (id, con) =>
          Frag(deleteByIdSql, IArray(id), idWriter(id)).update
            .run()(using con)
          ()
      else (_, _) => ()

    val deleteAllByIdImpl: (Iterable[ID], DbCon) => BatchUpdateResult =
      if hasId then
        (ids, con) =>
          given DbCon = con
          handleQuery(deleteByIdSql, ids):
            Using(con.connection.prepareStatement(deleteByIdSql)): ps =>
              idCodec.write(ids, ps)
              timed(batchUpdateResult(ps.executeBatch()))
      else (_, _) => BatchUpdateResult.Success(0)

    new RepoDefaults[EC, E, ID]:
      def count(using con: DbCon): Long = countQuery.run().head

      def existsById(id: ID)(using con: DbCon): Boolean =
        existsByIdImpl(id, con)

      def findAll(using DbCon): Vector[E] = findAllQuery.run()

      def findAll(spec: Spec[E])(using DbCon): Vector[E] =
        SpecImpl.Default.findAll(spec, tableNameSql)

      def findById(id: ID)(using con: DbCon): Option[E] =
        findByIdImpl(id, con)

      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
        throw UnsupportedOperationException()

      def delete(entity: E)(using DbCon): Unit =
        deleteById(entityToId(entity))

      def deleteById(id: ID)(using con: DbCon): Unit =
        deleteByIdImpl(id, con)

      def truncate()(using DbCon): Unit =
        truncateUpdate.run()

      def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult =
        deleteAllById(entities.map(entityToId))

      def deleteAllById(ids: Iterable[ID])(using
          con: DbCon
      ): BatchUpdateResult =
        deleteAllByIdImpl(ids, con)

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
          Using(con.connection.prepareStatement(insertSql)): ps =>
            ecCodec.writeSingle(entityCreator, ps)
            timed:
              ps.executeUpdate()
              entityCreator.asInstanceOf[E]

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        handleQuery(insertSql, entityCreators):
          Using(con.connection.prepareStatement(insertSql)): ps =>
            ecCodec.write(entityCreators, ps)
            timed:
              batchUpdateResult(ps.executeBatch())
              entityCreators.toVector.asInstanceOf[Vector[E]]

      def update(entity: E)(using DbCon): Unit =
        throw UnsupportedOperationException()

      def updateAll(entities: Iterable[E])(using
          con: DbCon
      ): BatchUpdateResult =
        throw UnsupportedOperationException()

      def entityToId(entity: E): ID =
        val product = entity.asInstanceOf[Product]
        val idValues = idIndices.map(i => product.productElement(i))
        idFromProduct(idValues)

    end new
  end buildRepoDefaults
end ClickhouseDbType
