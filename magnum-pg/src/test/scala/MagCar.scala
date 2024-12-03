import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.PostgresDbType
import com.augustnagro.magnum.pg.PgCodec.given
import com.augustnagro.magnum.pg.enums.PgStringToScalaEnumSqlArrayCodec
import com.augustnagro.magnum.repo.{Id, SqlNameMapper, Table}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class MagCar(
    @Id id: Long,
    textColors: Seq[Color],
    textColorMap: Vector[List[Color]],
    lastService: Option[LastService],
    myJsonB: Option[MyJsonB],
    myXml: Option[MyXml]
) derives DbCodec
