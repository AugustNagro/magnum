import com.augustnagro.magnum.{
  DbCodec,
  PostgresDbType,
  SqlName,
  SqlNameMapper,
  Table
}

@SqlName("colour")
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
enum Color derives DbCodec:
  case RedOrange
  @SqlName("Greenish") case Green
  case Blue
