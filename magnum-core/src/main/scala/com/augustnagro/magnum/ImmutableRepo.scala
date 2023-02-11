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
open class ImmutableRepo[E <: Product: DbEntity, ID](
    val ds: DataSource,
    val config: DbConfig[?, E, ID]
):

  // todo change somehow
//  val cols = Util.buildCols[E](config.tableName, config.sqlNameMapper)

  extension (sc: StringContext)
    def sql(args: Any*): SqlBuilder =
      if args.isEmpty then return SqlBuilder(sc.parts.mkString, Vector.empty)
      val resQuery = StringBuilder() ++= sc.parts(0)
      val resParams = Vector.newBuilder[Any]
      for i <- 0 until args.length do
        resQuery ++= sc.parts(i + 1)
        args(i) match
          case Col(columnName) => resQuery ++= columnName
          case param           => resParams += param
      SqlBuilder(resQuery.result(), resParams.result())

  def connect[T](f: DbCon ?=> T): T =
    Using
      .Manager(manager =>
        val con = manager(ds.getConnection)
        f(using DbCon(con, manager))
      )
      .get

  def transact[T](f: DbTx ?=> T): T =
    Using
      .Manager(manager =>
        val con = manager(ds.getConnection)
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
  def count(using DbCon): Long = config.count

  /** Returns true if an E exists with the given id */
  def existsById(id: ID)(using DbCon): Boolean = config.existsById(id)

  /** Returns all entity values */
  def findAll(using DbCon): Vector[E] = config.findAll

  /** Find all entities matching the specification. See the scaladoc of [[Spec]]
    * for more details
    */
  def findAll(spec: Spec[E])(using DbCon): Vector[E] = config.findAll(spec)

  /** Returns Some(entity) if a matching E is found */
  def findById(id: ID)(using DbCon): Option[E] = config.findById(id)

  /** Find all entities having ids in the Iterable. If an Id is not found, no
    * error is thrown.
    */
  def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
    config.findAllById(ids)
