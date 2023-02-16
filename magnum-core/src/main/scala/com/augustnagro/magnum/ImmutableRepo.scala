package com.augustnagro.magnum

import java.sql.ResultSet
import javax.sql.DataSource
import scala.util.{Try, Using}

/** Repository supporting read-only queries. When entity `E` does not have an
  * id, use `Null` for the `Id` type.
  * @tparam E
  *   database entity class
  * @tparam ID
  *   id type of E
  */
open class ImmutableRepo[E, ID](
    val dataSource: DataSource,
    val schema: DbSchema[?, E, ID]
):
  
  export Util.{sql, runPreparedBatch, runBatch}

  def connect[T](f: DbCon ?=> T): T =
    Using
      .Manager(manager =>
        val con = manager(dataSource.getConnection)
        f(using DbCon(con, manager))
      )
      .get

  def transact[T](f: DbTx ?=> T): T =
    Using
      .Manager(manager =>
        val con = manager(dataSource.getConnection)
        con.setAutoCommit(false)
        try
          val res = f(using DbTx(con, manager))
          con.commit()
          res
        catch
          case t =>
            con.rollback()
            throw t
      )
      .get

  /** Count of all entities */
  def count(using DbCon): Long = schema.count

  /** Returns true if an E exists with the given id */
  def existsById(id: ID)(using DbCon): Boolean = schema.existsById(id)

  /** Returns all entity values */
  def findAll(using DbCon): Vector[E] = schema.findAll

  /** Find all entities matching the specification. See the scaladoc of [[Spec]]
    * for more details
    */
  def findAll(spec: Spec[E])(using DbCon): Vector[E] = schema.findAll(spec)

  /** Returns Some(entity) if a matching E is found */
  def findById(id: ID)(using DbCon): Option[E] = schema.findById(id)

  /** Find all entities having ids in the Iterable. If an Id is not found, no
    * error is thrown.
    */
  def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
    schema.findAllById(ids)
