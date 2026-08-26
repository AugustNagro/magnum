package com.augustnagro.magnum

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object MySqlDbType extends DbType:

  private val specImpl = new SpecImpl:
    override def sortSql(sort: Sort): String =
      val column = sort.column
      val nullSort = sort.nullOrder match
        case NullOrder.Default => ""
        case NullOrder.First   => s"$column IS NOT NULL, "
        case NullOrder.Last    => s"$column IS NULL, "
        case _                 => throw UnsupportedOperationException()
      val dir = sort.direction match
        case SortOrder.Default => ""
        case SortOrder.Asc     => " ASC"
        case SortOrder.Desc    => " DESC"
        case _                 => throw UnsupportedOperationException()
      nullSort + column + dir

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
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")
    val idCodecs =
      if idIndices.nonEmpty then idIndices.map(eElemCodecs).toVector
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

    def entityToId(entity: E): ID =
      val product = entity.asInstanceOf[Product]
      val idValues = idIndices.map(i => product.productElement(i))
      idFromProduct(idValues)

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
          "MySql does not support 'ANY' keyword, and does not support long IN parameter lists. Use findById in a loop instead."
        )

      def delete(entity: E)(using DbCon): Unit =
        deleteById(entityToId(entity))

      def deleteById(id: ID)(using DbCon): Unit =
        Frag(deleteByIdSql, IArray(id), idWriter(id)).update
          .run()

      def truncate()(using DbCon): Unit = truncateUpdate.run()

      def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult =
        deleteAllById(entities.map(entityToId))

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
        // unfortunately, mysql only will return auto_incremented keys.
        // it doesn't return default columns, and adding other columns to
        // the insertGenKeys array doesn't change this behavior.
        throw UnsupportedOperationException()

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon): Vector[E] =
        throw UnsupportedOperationException()

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
    end new
  end buildRepoDefaults
end MySqlDbType
