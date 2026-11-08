import com.augustnagro.magnum.*
import com.augustnagro.magnum.pg.PgCodec.given
import java.util.UUID

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class PgOptionalArrays(
    @Id id: UUID,
    vectorValues: Option[Vector[Long]],
    iArrayValues: Option[IArray[Int]],
    arrayValues: Option[Array[Int]]
) derives DbCodec
