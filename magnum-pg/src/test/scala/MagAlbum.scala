import com.augustnagro.magnum.*
import com.augustnagro.magnum.pg.PgCodec.given

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class MagAlbum(@Id id: Long, myVec: Option[Vector[Int]]) derives DbCodec

case class MagAlbumCreator(myVec: Option[Vector[Int]]) derives DbCodec
