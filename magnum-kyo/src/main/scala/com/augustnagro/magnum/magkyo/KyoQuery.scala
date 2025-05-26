package com.augustnagro.magnum.magkyo

import com.augustnagro.magnum.*
import kyo.*
import scala.util.Using
import java.sql.PreparedStatement
import java.sql.ResultSet

extension [Out](q: Query[Out]) {
  def stream(
      fetchSize: Int = 512
  )(using con: DbCon, tagOut: Tag[Out]): Stream[Out, Resource & IO] =
    def toEmit(
        it: ResultSetIterator[Out],
        size: Int
    ): Unit < (Emit[Chunk[Out]] & IO) =
      IO {
        val builder = Chunk.newBuilder[Out]
        var curSize = 0
        while it.hasNext && curSize < size do
          builder += it.next()
          curSize += 1

        Emit
          .value(builder.result())
          .andThen(
            if it.hasNext then toEmit(it, size)
            else ()
          )
      }

    val preparedStatement: PreparedStatement < (Resource & IO) = Resource
      .acquireRelease(
        IO(con.connection.prepareStatement(q.frag.sqlString))
      )(con => IO(con.close()))

    val resultSetEmit: Unit < (Emit[Chunk[Out]] & Resource & IO) =
      preparedStatement
        .map { ps =>
          val resultSet: ResultSet < (Resource & IO) = Resource
            .acquireRelease {
              IO {
                ps.setFetchSize(fetchSize)
                q.frag.writer.write(ps, 1)
                ps.executeQuery()
              }
            }(rs => IO(rs.close()))

          resultSet.map(rs =>
            toEmit(
              ResultSetIterator(rs, q.frag, q.reader, con.sqlLogger),
              fetchSize
            )
          )
        }

    Stream(resultSetEmit)
  end stream
}
