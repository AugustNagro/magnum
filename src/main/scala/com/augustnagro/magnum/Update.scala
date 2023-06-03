package com.augustnagro.magnum

import scala.util.{Failure, Success, Using}

case class Update(frag: Frag):
  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def runUpdate(using con: DbCon): Int =
    logSql(frag)
    Using(con.connection.prepareStatement(frag.query))(ps =>
      setValues(ps, frag.params)
      ps.executeUpdate()
    ) match
      case Success(res) => res
      case Failure(t)   => throw SqlException(frag.query, frag.params, t)
