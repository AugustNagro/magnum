package com.augustnagro.magnum

import java.sql.PreparedStatement

trait FragWriter:
  /** Writes a Frag's values to `ps`, staring at postion `pos`. Returns the new position. */
  def write(ps: PreparedStatement, pos: Int): Int
