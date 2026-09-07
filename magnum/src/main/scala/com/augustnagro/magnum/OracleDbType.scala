package com.augustnagro.magnum

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object OracleDbType extends DbType:

  private val specImpl = new SpecImpl:
    override def offsetLimitSql(
        offset: Option[Long],
        limit: Option[Int]
    ): Option[String] =
      (offset, limit) match
        case (Some(o), Some(l)) =>
          Some(s"OFFSET $o ROWS FETCH NEXT $l ROWS ONLY")
        case (Some(o), None) => Some(s"OFFSET $o ROWS")
        case (None, Some(l)) => Some(s"FETCH NEXT $l ROWS ONLY")
        case (None, None)    => None

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
    val idNames = idIndices.map(eElemNamesSql)
    val hasId = idIndices.nonEmpty
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")
    val idCodecs =
      if hasId then idIndices.map(eElemCodecs).toVector
      else Vector(idCodec)

    val updateKeys: String = eElemNamesSql
      .zip(eElemCodecs)
      .zipWithIndex
      .collect {
        case ((sqlName, codec), idx) if !idIndices.contains(idx) =>
          sqlName + " = " + codec.queryRepr
      }
      .mkString(", ")

    val updateCodecs: IArray[DbCodec[Any]] = IArray.from(
      eElemCodecs.zipWithIndex
        .collect { case (codec, idx) if !idIndices.contains(idx) => codec }
        .appendedAll(idCodecs)
        .map(_.asInstanceOf[DbCodec[Any]])
    )
    val updateParamIndices: IArray[Int] = IArray.from(
      eElemNames.indices.filterNot(idIndices.contains) ++ idIndices
    )

    val idWhereClause = idNames match
      case Seq() => "1 = 0"
      case _ =>
        idNames
          .zip(idCodecs)
          .map((name, codec) => name + " = " + codec.queryRepr)
          .mkString(" AND ")

    val insertGenKeys = Array.from(eElemNamesSql)

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
    val updateSql =
      s"UPDATE $tableNameSql SET $updateKeys WHERE $idWhereClause"

    val idFirstTypeName = JDBCType.valueOf(idCodec.cols.head).getName

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

    def writeUpdateParams(entity: E, ps: PreparedStatement): Unit =
      val product = entity.asInstanceOf[Product]
      var pos = 1
      var i = 0
      while i < updateParamIndices.length do
        val codec = updateCodecs(i)
        codec.writeSingle(
          product.productElement(updateParamIndices(i)),
          ps,
          pos
        )
        pos += codec.cols.length
        i += 1

    new RepoDefaults[EC, E, ID]:
      def count(using con: DbCon): Long = countQuery.run().head

      def existsById(id: ID)(using con: DbCon): Boolean =
        existsByIdImpl(id, con)

      def findAll(using DbCon): Vector[E] = findAllQuery.run()

      def findAll(spec: Spec[E])(using DbCon): Vector[E] =
        specImpl.findAll(spec, tableNameSql)

      def findById(id: ID)(using con: DbCon): Option[E] =
        findByIdImpl(id, con)

      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
        throw UnsupportedOperationException(
          "Oracle does not support SQL arrays, and does not support long IN parameter lists. Use findById in a loop instead."
        )

      def delete(entity: E)(using DbCon): Unit =
        deleteById(entityToId(entity))

      def deleteById(id: ID)(using con: DbCon): Unit =
        deleteByIdImpl(id, con)

      def truncate()(using DbCon): Unit = truncateUpdate.run()

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
        // oracle jdbc does not support batch RETURNING
        entityCreators.map(insertReturning).toVector

      def update(entity: E)(using con: DbCon): Unit =
        handleQuery(updateSql, entity):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            writeUpdateParams(entity, ps)
            timed(ps.executeUpdate())

      def updateAll(entities: Iterable[E])(using
          con: DbCon
      ): BatchUpdateResult =
        handleQuery(updateSql, entities):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            for entity <- entities do
              writeUpdateParams(entity, ps)
              ps.addBatch()

            timed(batchUpdateResult(ps.executeBatch()))

      def entityToId(entity: E): ID =
        val product = entity.asInstanceOf[Product]
        val idValues = idIndices.map(i => product.productElement(i))
        idFromProduct(idValues)

    end new
  end buildRepoDefaults
end OracleDbType
