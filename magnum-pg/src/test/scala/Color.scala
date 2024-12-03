import com.augustnagro.magnum.codec.DbCodec
import com.augustnagro.magnum.dbtype.PostgresDbType
import com.augustnagro.magnum.repo.{SqlName, SqlNameMapper, Table}

@SqlName("colour")
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
enum Color derives DbCodec:
  case RedOrange
  @SqlName("Greenish") case Green
  case Blue
