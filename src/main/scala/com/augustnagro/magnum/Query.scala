package com.augustnagro.magnum

import scala.util.{Failure, Success, Using}

case class Query[E](frag: Frag, reader: DbCodec[E]):

  def run()(using con: DbCon): Vector[E] =
    logSql(frag)
    Using.Manager(use =>
      val ps = use(con.connection.prepareStatement(frag.sqlString))
      frag.writer.write(ps, 1)
      val rs = use(ps.executeQuery())
      reader.read(rs)
    ) match
      case Success(res) => res
      case Failure(t)   => throw SqlException(frag, t)
