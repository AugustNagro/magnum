package com.augustnagro.magnum

import java.sql.ResultSet

trait RowConverter[E] {

  /** Build an E from the ResultSet. Make sure the ResultSet is in a valid state
    * (ie, ResultSet::next has been called).
    */
  def buildSingle(rs: ResultSet): E

  /** Build every row in the ResultSet into a sequence of E. The ResultSet
    * should be in its initial position before calling (ie, ResultSet::next not
    * called).
    */
  def build(rs: ResultSet): Vector[E] =
    val res = Vector.newBuilder[E]
    while rs.next() do res += buildSingle(rs)
    res.result()

}
