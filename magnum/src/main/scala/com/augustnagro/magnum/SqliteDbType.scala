package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object SqliteDbType extends DbType:

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

    val insertGenKeys = eElemNamesSql.toArray

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql).query[E]
    val findByIdSql =
      s"SELECT * FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val truncateSql = s"DELETE FROM $tableNameSql"
    val truncateUpdate = Frag(truncateSql).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"
    val updateSql =
      s"UPDATE $tableNameSql SET $updateKeys WHERE $idName = ${idCodec.queryRepr}"

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
        Using.Manager(use =>
          val ps =
            use(
              con.connection
                .prepareStatement(
                  insertSql,
                  Statement.RETURN_GENERATED_KEYS
                )
            )
          ecCodec.writeSingle(entityCreator, ps)
          ps.executeUpdate()
          val rs = use(ps.getGeneratedKeys)
          rs.next()
          val id = idCodec.readSingle(rs)
          // unfortunately, sqlite only will return the primary key.
          // it doesn't return default columns, and adding other columns to
          // the insertGenKeys array doesn't change this behavior. So we need
          // to query by ID after inserting.
          findById(id).get
        ) match
          case Success(res) => res
          case Failure(t) =>
            throw SqlException(insertSql, entityCreator, t)
        end match
      end insertReturning

      // todo
      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        throw UnsupportedOperationException()
//        logSql(insertSql, entityCreators)
//        Using.Manager(use =>
//          val ps =
//            use(
//              con.connection
//                .prepareStatement(
//                  insertSql,
//                  Statement.RETURN_GENERATED_KEYS
//                )
//            )
//          ecCodec.write(entityCreators, ps)
//          batchUpdateResult(ps.executeBatch())
//          val rs = use(ps.getGeneratedKeys)
//          idCodec.read(rs).map(findById(_).get)
//        ) match
//          case Success(res) => res
//          case Failure(t) =>
//            throw SqlException(insertSql, entityCreators, t)

      def update(entity: E)(using con: DbCon): Unit =
        logSql(updateSql, entity)
        Using(con.connection.prepareStatement(updateSql))(ps =>
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
          ps.executeUpdate()
        ) match
          case Success(_) => ()
          case Failure(t) => throw SqlException(updateSql, entity, t)
      end update

      def updateAll(entities: Iterable[E])(using
          con: DbCon
      ): BatchUpdateResult =
        logSql(updateSql, entities)
        Using(con.connection.prepareStatement(updateSql))(ps =>
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

          batchUpdateResult(ps.executeBatch())
        ) match
          case Success(res) => res
          case Failure(t)   => throw SqlException(updateSql, entities, t)
        end match
      end updateAll
    end new
  end buildRepoDefaults
end SqliteDbType
