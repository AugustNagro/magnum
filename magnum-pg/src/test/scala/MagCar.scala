import com.augustnagro.magnum.{
  DbCodec,
  Id,
  PostgresDbType,
  SqlNameMapper,
  Table
}
import com.augustnagro.magnum.pg.PgCodec.given
import com.augustnagro.magnum.pg.enums.PgStringToScalaEnumSqlArrayCodec

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class MagCar(
    @Id id: Long,
    textColors: Seq[Color],
    textColorMap: Vector[List[Color]]
) derives DbCodec
