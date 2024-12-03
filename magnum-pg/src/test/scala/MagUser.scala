import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.PostgresDbType
import com.augustnagro.magnum.pg.PgCodec.given
import com.augustnagro.magnum.pg.enums.PgEnumToScalaEnumSqlArrayCodec
import com.augustnagro.magnum.repo.{Id, SqlName, Table}
import org.postgresql.geometric.{
  PGbox,
  PGcircle,
  PGline,
  PGlseg,
  PGpath,
  PGpoint,
  PGpolygon
}
import org.postgresql.util.PGInterval

import java.time.OffsetDateTime
import java.util.Objects

@Table(PostgresDbType)
@SqlName("mag_user")
case class MagUser(
    @Id id: Long,
    name: String,
    friends: Vector[String],
    matrix: IArray[IArray[Int]],
    test: IArray[Int],
    dates: IArray[OffsetDateTime],
    bx: PGbox,
    c: PGcircle,
    iv: PGInterval,
    l: PGline,
    lSeg: PGlseg,
    p: PGpath,
    pnt: PGpoint,
    poly: PGpolygon,
    colors: List[Color],
    colorMap: List[Vector[Color]]
) derives DbCodec:
  override def equals(obj: Any): Boolean =
    obj match
      case u: MagUser =>
        id == u.id && name == u.name && friends == u.friends &&
        Objects.deepEquals(matrix, u.matrix) &&
        Objects.deepEquals(test, u.test) &&
        Objects.deepEquals(dates, u.dates) &&
        bx == u.bx && c == u.c && iv == u.iv && l == u.l && lSeg == u.lSeg &&
        p == u.p && pnt == u.pnt && poly == u.poly &&
        colors == u.colors && colorMap == u.colorMap
      case _ => false
end MagUser
