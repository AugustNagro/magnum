package com.augustnagro.magnum

import scala.util.{Failure, Success, Using}

case class Update(frag: Frag):
  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def run()(using con: DbCon): Int =
    logSql(frag)
    Using(con.connection.prepareStatement(frag.sqlString))(ps =>
      frag.writer(ps, 1)
      ps.executeUpdate()
    ) match
      case Success(res) => res
      case Failure(t)   => throw SqlException(frag, t)
