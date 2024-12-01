package com.augustnagro.magnum

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Using}

class Update private[magnum] (val frag: Frag):
  /** Exactly like [[java.sql.PreparedStatement]].executeUpdate */
  def run()(using con: DbCon): Int =
    handleQuery(frag.sqlString, frag.params):
      Using(con.connection.prepareStatement(frag.sqlString)): ps =>
        frag.writer.write(ps, 1)
        timed(ps.executeUpdate())
