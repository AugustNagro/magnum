package com.augustnagro.magnum

import scala.concurrent.duration.FiniteDuration

/** Metadata for a successfully executed SQL statement. */
class SqlSuccessEvent private[magnum] (
    /** The SQL String */
    val sql: String,
    anyParams: Any,
    /** Time taken to execute the query, fetch data, and build the results. Does
      * not include time to construct the preparedStatement. For streaming
      * methods like `Query.iterator`, sqlExecTime is only calculated for the
      * first fetch.
      */
    val execTime: FiniteDuration
):
  /** The parameters used when executing. The type is `Iterator[Iterator[Any]]`
    * to support logging batched updates. For example,
    * {{{
    *   repo.insert(User(a, b, c)) // provides Iterator(Iterator(a, b, c))
    *   repo.insertAll(List(User(a, b, c), User(d, e, f))) // provides Iterator(Iterator(a, b, c), Iterator(d, e, f))
    * }}}
    */
  def params: Iterator[Iterator[Any]] = parseParams(anyParams)
