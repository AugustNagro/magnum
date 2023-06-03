package com.augustnagro.magnum

import scala.util.{Failure, Success, Using}

case class Query[E](frag: Frag, codec: DbCodec[E]):

  def run(using con: DbCon): Vector[E] =
    logSql(frag)
    Using.Manager(use =>
      val ps = use(con.connection.prepareStatement(frag.query))
      setValues(ps, frag.params)
      val rs = use(ps.executeQuery())
      codec.read(rs)
    ) match
      case Success(res) => res
      case Failure(t) => throw SqlException(frag.query, frag.params, t)
