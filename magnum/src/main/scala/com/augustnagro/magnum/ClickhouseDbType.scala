package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
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
      idIndex: Int
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
    val idName = eElemNamesSql(idIndex)
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllSql = s"SELECT $selectKeys FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql).query[E]
    val findByIdSql =
      s"SELECT $selectKeys FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val truncateUpdate = Frag(truncateSql).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"

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
        val f = spec.build
        Frag(s"SELECT * FROM $tableNameSql ${f.sqlString}", f.params, f.writer)
          .query[E]
          .run()

      def findById(id: ID)(using DbCon): Option[E] =
        Frag(findByIdSql, IArray(id), idWriter(id))
          .query[E]
          .run()
          .headOption

      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
        throw UnsupportedOperationException()

      def delete(entity: E)(using DbCon): Unit =
        deleteById(
          entity
            .asInstanceOf[Product]
            .productElement(idIndex)
            .asInstanceOf[ID]
        )

      def deleteById(id: ID)(using DbCon): Unit =
        Frag(deleteByIdSql, IArray(id), idWriter(id)).update
          .run()

      def truncate()(using DbCon): Unit =
        truncateUpdate.run()

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
          idCodec.write(ids, ps)
          batchUpdateResult(ps.executeBatch())
        ) match
          case Success(res) => res
          case Failure(t)   => throw SqlException(deleteByIdSql, ids, t)

      def insert(entityCreator: EC)(using con: DbCon): Unit =
        logSql(insertSql, entityCreator)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          ecCodec.writeSingle(entityCreator, ps)
          ps.executeUpdate()
        ) match
          case Success(_)  => ()
          case Failure(ex) => throw SqlException(insertSql, entityCreator, ex)

      def insertAll(entityCreators: Iterable[EC])(using con: DbCon): Unit =
        logSql(insertSql, entityCreators)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          ecCodec.write(entityCreators, ps)
          batchUpdateResult(ps.executeBatch())
        ) match
          case Success(_) => ()
          case Failure(t) => throw SqlException(insertSql, entityCreators, t)

      def insertReturning(entityCreator: EC)(using con: DbCon): E =
        logSql(insertSql, entityCreator)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          ecCodec.writeSingle(entityCreator, ps)
          ps.executeUpdate()
          entityCreator.asInstanceOf[E]
        ) match
          case Success(res) => res
          case Failure(t)   => throw SqlException(insertSql, entityCreator, t)

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        logSql(insertSql, entityCreators)
        Using(con.connection.prepareStatement(insertSql))(ps =>
          ecCodec.write(entityCreators, ps)
          batchUpdateResult(ps.executeBatch())
          entityCreators.toVector.asInstanceOf[Vector[E]]
        ) match
          case Success(res) => res
          case Failure(t)   => throw SqlException(insertSql, entityCreators, t)

      def update(entity: E)(using DbCon): Unit =
        throw UnsupportedOperationException()

      def updateAll(entities: Iterable[E])(using
          con: DbCon
      ): BatchUpdateResult =
        throw UnsupportedOperationException()

      def columns: AllColumns = AllColumns.fromSeq(eElemNamesSql)

      def insertColumns: InsertColumns = InsertColumns.fromSeq(ecElemNamesSql)

      def tableName: Repo.TableName = Repo.TableName(tableNameSql)

      def idColumn: Repo.IdColumn = Repo.IdColumn(idName)
