package com.augustnagro.magnum

/** Metadata for a exceptional SQL statement. */
class SqlExceptionEvent private[magnum] (
    /** The SQL string */
    val sql: String,
    anyParams: Any,
    /** Cause of the exception */
    val cause: Throwable
):
  /** The parameters used when executing. The type is `Iterator[Iterator[Any]]`
    * to support logging batched updates. For example,
    * {{{
    *   repo.insert(User(a, b, c)) // provides Iterator(Iterator(a, b, c))
    *   repo.insertAll(List(User(a, b, c), User(d, e, f))) // provides Iterator(Iterator(a, b, c), Iterator(d, e, f))
    * }}}
    */
  def params: Iterator[Iterator[Any]] = parseParams(anyParams)
