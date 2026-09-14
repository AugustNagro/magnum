package com.augustnagro.magnum

import scala.reflect.ClassTag
import scala.util.Using

object MsSqlDbType extends DbType:

  /** SQL Server allows at most 2100 parameters per statement. `findAllById`
    * chunks its id list to this size, leaving some headroom under that cap.
    */
  private val maxInParams = 2000

  private val specImpl = new SpecImpl:
    // SQL Server has no NULLS FIRST/LAST. MySql emulates this with a leading
    // `col IS NULL, ` sort key, but T-SQL has no boolean expression value,
    // so a CASE expression is needed instead.
    override def sortSql(sort: Sort): String =
      val nullSort = sort.nullOrder match
        case NullOrder.Default => ""
        case NullOrder.First =>
          s"CASE WHEN ${sort.column} IS NULL THEN 0 ELSE 1 END, "
        case NullOrder.Last =>
          s"CASE WHEN ${sort.column} IS NULL THEN 1 ELSE 0 END, "
        case _ => throw UnsupportedOperationException()
      val dir = sort.direction match
        case SortOrder.Default => ""
        case SortOrder.Asc     => " ASC"
        case SortOrder.Desc    => " DESC"
        case _                 => throw UnsupportedOperationException()
      nullSort + sort.column + dir

    // T-SQL requires OFFSET before FETCH NEXT.
    override def offsetLimitSql(
        offset: Option[Long],
        limit: Option[Int]
    ): Option[String] =
      (offset, limit) match
        case (Some(o), Some(l)) =>
          Some(s"OFFSET $o ROWS FETCH NEXT $l ROWS ONLY")
        case (Some(o), None) => Some(s"OFFSET $o ROWS")
        case (None, Some(l)) => Some(s"OFFSET 0 ROWS FETCH NEXT $l ROWS ONLY")
        case (None, None)    => None

    // T-SQL rejects OFFSET/FETCH without an ORDER BY.
    override def orderByFallback: Option[String] = Some(
      "ORDER BY (SELECT NULL)"
    )

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

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql, Vector.empty, FragWriter.empty).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllSql = s"SELECT $selectKeys FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql, Vector.empty, FragWriter.empty).query[E]
    val findByIdSql =
      s"SELECT $selectKeys FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
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
        specImpl.findAll(spec, tableNameSql)

      def findById(id: ID)(using DbCon): Option[E] =
        Frag(findByIdSql, IArray(id), idWriter(id))
          .query[E]
          .run()
          .headOption

      // SQL Server has no 'ANY' keyword, so the IN list is built per call.
      // Long id lists are split across several statements to stay under the
      // parameter limit. Like Postgres' `= ANY(?)`, no result order is
      // guaranteed, so the chunks can simply be concatenated.
      def findAllByIdChunk(idChunk: Seq[ID])(using DbCon): Vector[E] =
        val placeholders =
          Vector.fill(idChunk.size)(idCodec.queryRepr).mkString(", ")
        val findAllByIdSql =
          s"SELECT $selectKeys FROM $tableNameSql WHERE $idName IN ($placeholders)"
        Frag(
          findAllByIdSql,
          idChunk,
          (ps, startingPos) =>
            var pos = startingPos
            for id <- idChunk do
              idCodec.writeSingle(id, ps, pos)
              pos += idCodec.cols.length
            pos
        ).query[E].run()

      def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
        if compositeId then
          throw UnsupportedOperationException(
            "Composite ids unsupported for findAllById."
          )
        ids.iterator
          .grouped(maxInParams)
          .flatMap(findAllByIdChunk)
          .toVector

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
        /** https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver16#triggers
          *
          * If the OUTPUT clause is specified without also specifying the INTO
          * keyword, the target of the DML operation can't have any enabled
          * trigger defined on it for the given DML action.
          *
          * An UPDATE, INSERT, or DELETE statement that has an OUTPUT clause
          * will return rows to the client even if the statement encounters
          * errors and is rolled back. The result shouldn't be used if any error
          * occurs.
          *
          * Conclusion: This should not be implemented as a default. It is
          * implementable, if you know your DB does not have certain trigers
          * enabled
          */
        throw UnsupportedOperationException()

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
end MsSqlDbType
