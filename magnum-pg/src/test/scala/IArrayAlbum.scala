import com.augustnagro.magnum.*
import com.augustnagro.magnum.pg.PgCodec.given
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase) // This will target "mag_iarray_album" by default
case class IArrayAlbum(@Id id: Long, myIarray: Option[IArray[Int]]) derives DbCodec

case class IArrayAlbumCreator(myIarray: Option[IArray[Int]]) derives DbCodec