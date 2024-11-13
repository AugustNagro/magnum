## Postgres Module

The Postgres Module adds support for [Geometric Types](https://www.postgresql.org/docs/current/datatype-geometric.html), [Arrays](https://www.postgresql.org/docs/current/arrays.html), [Json/JsonB](https://www.postgresql.org/docs/current/datatype-json.html), and [xml](https://www.postgresql.org/docs/current/datatype-xml.html). Postgres Arrays can be decoded into Scala List/Vector/IArray, etc; multi-dimensionality is also supported.

```
"com.augustnagro" %% "magnumpg" % "1.2.1"
```

Example: Insert into a table with a `point[]` type column.

With table:

```sql
create table my_geo (
  id bigint primary key,
  pnts point[] not null
);
```

```scala
import org.postgresql.geometric.PGpoint
import com.augustnagro.magnum.{Table, PostgresDbType, Id, DbCodec, Transactor, Repo, transact}
import com.augustnagro.magnum.pg.PgCodec.given

@Table(PostgresDbType)
case class MyGeo(@Id id: Long, pnts: IArray[PGpoint]) derives DbCodec

val dataSource: javax.sql.DataSource = ???
val xa = Transactor(dataSource)

val myGeoRepo = Repo[MyGeo, MyGeo, Long]

transact(xa):
  myGeoRepo.insert(MyGeo(1L, IArray(PGpoint(1, 1), PGPoint(2, 2))))
```

The import of `PgCodec.given` is required to bring Geo/Array DbCodecs into scope.

#### Arrays of Enums

The `pg` module supports arrays of simple (non-ADT) enums.

If you want to map an array of [Postgres enums](https://www.postgresql.org/docs/current/datatype-enum.html) to a sequence of Scala enums, use the following import when deriving the DbCodec:

```scala
import com.augustnagro.magnum.pg.PgCodec.given
import com.augustnagro.magnum.pg.enums.PgEnumToScalaEnumSqlArrayCodec

// in postgres: `create type Color as enum ('Red', 'Green', 'Blue');`
enum Color derives DbCodec:
  case Red, Green, Blue

@Table(PostgresDbType)
case class Car(@Id id: Long, colors: Vector[Color]) derives DbCodec
```

If instead your Postgres type is an array of varchar or text, use the following import:

```scala
import com.augustnagro.magnum.pg.enums.PgStringToScalaEnumSqlArrayCodec
```

#### Json, JsonB, XML

You can map `json`, `jsonb`, and `xml` columns to Scala classes by implementing `JsonDbCodec`, `JsonBDbCodec`, and `XmlDbCodec` respectively.

As an example, assume we have table `car`:

```sql
CREATE TABLE car (
  id bigint primary key,
  last_service json not null
);
```

And `last_service` looks like:

```json
{"mechanic": "Bob", "date":  "2024-05-04"}
```

We can model the relation in Scala with:

```scala
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class Car(
    @Id id: Long,
    lastService: LastService
) derives DbCodec

case class LastService(mechanic: String, date: LocalDate)
```

However, this won't compile because we're missing a given `DbCodec[LastService]`. To get there, first we have to pick a Scala JSON library. Nearly all of them support creating derived codecs; the example below shows how it's done in [Circe](https://circe.github.io/circe):

```scala
import io.circe.Codec
import java.time.LocalDate

case class LastService(mechanic: String, date: LocalDate) derives Codec.AsObject
```

Next, we should extend `JsonDbCodec` to implement our own `CirceDbCodec`:

```scala
import com.augustnagro.magnum.pg.json.JsonDbCodec
import io.circe.{Codec, Decoder, Encoder, JsonObject}
import io.circe.parser.{decode as circeDecode, *}
import io.circe.syntax.*

trait CirceDbCodec[A] extends JsonDbCodec[A]

object CirceDbCodec:
  def derived[A: Encoder: Decoder]: CirceDbCodec[A] = new:
    def encode(a: A): String = a.asJson.toString
    def decode(json: String): A = circeDecode[A](json) match
      case Right(a)  => a
      case Left(err) => throw err
```

Note the `derived` method in the companion object; this allows us to use `derives PlayJsonDbCodec` on our JSON class, like so:

```scala
case class LastService(mechanic: String, date: LocalDate)
  derives Codec.AsObject, CirceDbCodec
```

The `Car` example will now compile and work as expected.

For XML, there a few options. If using a library that maps XML to case classes like [scalaxb](https://github.com/eed3si9n/scalaxb), we can follow the JSON pattern above, but using `XmlDbCodec`. If the case classes are generated sources, we can't put the DbCodec givens in their companion objects. Instead, put them in the entity companion object.

Another pattern is to use a library like [scala-xml](https://github.com/scala/scala-xml) directly and encapsulate the NodeSeq. Then, we can define our DbCodec on the wrapper:

```scala
class LastService(val xml: Elem):
  def mechanic: String = (xml \ "mechanic").head.text.trim
  def date: LocalDate = LocalDate.parse((xml \ "date").head.text.trim)

object LastService:
  given XmlDbCodec[LastService] with
    def encode(a: LastService): String = a.xml.toString
    def decode(xml: String): LastService = LastService(XML.loadString(xml))
```
