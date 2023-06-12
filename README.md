## Magnum

Yet another database client for Scala. No dependencies, high productivity.

* [Installing](#installing)
* [ScalaDoc](#scaladoc)
* [Documentation](#documentation)
  * [`connect` creates a database connection](#connect-creates-a-database-connection)
  * [`transact` creates a database transaction](#transact-creates-a-database-transaction)
  * [Type-safe Transaction & Connection Management](#type-safe-transaction--connection-management)
  * [Customizing the transaction's JDBC Connection](#customizing-the-transactions-jdbc-connection)
  * [Sql Interpolator, Frag, Query, and Update](#sql-interpolator-frag-query-and-update)
  * [Batch Updates](#batch-updates)
  * [Immutable Repositories](#immutable-repositories)
  * [Repositories](#repositories)
  * [Database generated columns](#database-generated-columns)
  * [Specifications](#specifications)
  * [Scala 3 Enum Support](#scala-3-enum-support)
  * [`DbCodec`: Typeclass for JDBC reading & writing](#dbcodec-typeclass-for-jdbc-reading--writing)
  * [Logging](#logging-sql-queries)
* [Motivation](#motivation)
* [Feature List And Database Support](#feature-list)
* [Documentation](#documentation)
* [Talks and Presentations](#talks-and-presentations)

## Installing

```
"com.augustnagro" %% "magnum" % "1.0.0"
```

Magnum requires Scala >= 3.3.0

## ScalaDoc

https://javadoc.io/doc/com.augustnagro/magnum

## Documentation

### `connect` creates a database connection.

`connect` takes two parameters; the database DataSource,
and a context function with a given `DbCon` connection.
For example:

```scala
import com.augustnagro.magnum.*

val ds: DataSource = ???

val users: Vector[User] = connect(ds):
  sql"SELECT * FROM user".query[User].run()
```

### `transact` creates a database transaction.

Like `connect`, `transact` accepts a DataSource and context function.
The context function provides a `DbTx` instance.
If the function throws, the transaction will be rolled back.

```scala
// update is rolled back
transact(ds):
  sql"UPDATE user SET first_name = $firstName WHERE id = $id".update.run()
  thisMethodThrows()
```

### Type-safe Transaction & Connection Management

Annotate transactional methods with `using DbTx`, and connections with `using DbCon`.

Since `DbTx <: DbCon`, it's impossible to call a method with the wrong context.

For example, this compiles:

```scala
def runUpdateAndGetUsers()(using DbTx): Vector[User] =
  userRepo.deleteById(1L)
  getUsers

def getUsers(using DbCon): Vector[User] =
  sql"SELECT * FROM user".query.run()
```

But not this:

```scala
def runSomeQueries(using DbCon): Vector[User] =
  runUpdateAndGetUsers()
```

### Customizing the transaction's JDBC Connection.

`transact` lets you customize the underlying java.sql.Connection.

```scala
transact(ds(), withRepeatableRead):
  ???

def withRepeatableRead(con: Connection): Unit =
  con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
```

### Sql Interpolator, Frag, Query, and Update

The `sql` interpolator can express any SQL expression, returning a `Frag` sql fragment. You can interpolate values without the risk of SQL-injection attacks.

```scala
val firstNameOpt = Some("John")
val twoDaysAgo = OffsetDateTime.now.minusDays(2)

val frag: Frag =
  sql"""
    SELECT id, last_name FROM user
    WHERE first_name = $firstNameOpt
    AND created <= $twoDaysAgo
    """
```

Frags can be turned into queries with the `query[T](using DbCodec[T])` method:

```scala
val query = frag.query[(Long, String)] // Query[(Long, String)]
```

Or updates via `update`

```scala
val update: Update =
  sql"UPDATE user SET first_name = 'Buddha' WHERE id = 3".update
```

Both are executed via `run()(using DbCon)`:

```scala
transact(ds):
  val tuples: Vector[(Long, String)] = query.run()
  val updatedRows: Int = update.run()
```

### Batch Updates

Batch updates are supported via `batchUpdate` method in package `com.augustnagro.magnum`.

```scala
connect(ds):
  val users: Iterable[User] = ???
  val updateResult: BatchUpdateResult =
    batchUpdate(users): user =>
      sql"...".update
```

`batchUpdate` returns a `BatchUpdateResult` enum, which is `Success(numRowsUpdated)` or `SuccessNoInfo` otherwise.

### Immutable Repositories

The `ImmutableRepo` class auto-generates the following methods at compile-time:

```scala
  def count(using DbCon): Long
  def existsById(id: ID)(using DbCon): Boolean
  def findAll(using DbCon): Vector[E]
  def findAll(spec: Spec[E])(using DbCon): Vector[E]
  def findById(id: ID)(using DbCon): Option[E]
  def findAllById(ids: Iterable[ID])(using DbCon): Vector[E]
```

Here's an example:

```scala
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: Option[String],
  lastName: String,
  created: OffsetDateTime
) derives DbCodec

val userRepo = ImmutableRepo[User, Long]

transact(ds):
  val cnt = userRepo.count
  val userOpt = userRepo.findById(2L)
```

Importantly, class User is annotated with `@Table`, which defines the table's database type. The annot optionally specifies the name-mapping between the scala fields and column names. The table must also `derive DbCodec`, or otherwise provide an implicit DbCodec instance.

The optional `@Id` annotation denotes the table's primary key. Not setting `@Id` will default to using the first field. If there is no logical id, then strip the annotation and use Null in the ID type parameter of Repositories (see next).

It is a best practice to extend ImmutableRepo to encapsulate your SQL in repositories. This way, it's easier to maintain since they're grouped together.

```scala
class UserRepo extends ImmutableRepo[User, Long]:
  def firstNamesForLast(lastName: String)(using DbCon): Vector[String] =
    sql"""
      SELECT DISTINCT first_name
      FROM user
      WHERE last_name = $lastName
      """.query[String].run()
        
  // other User-related queries here
```

### Repositories

The `Repo` class auto-generates the following methods at compile-time:

```scala
  def count(using DbCon): Long
  def existsById(id: ID)(using DbCon): Boolean
  def findAll(using DbCon): Vector[E]
  def findAll(spec: Spec[E])(using DbCon): Vector[E]
  def findById(id: ID)(using DbCon): Option[E]
  def findAllById(ids: Iterable[ID])(using DbCon): Vector[E]
  
  def delete(entity: E)(using DbCon): Unit
  def deleteById(id: ID)(using DbCon): Unit
  def truncate()(using DbCon): Unit
  def deleteAll(entities: Iterable[E])(using DbCon): Unit
  def deleteAllById(ids: Iterable[ID])(using DbCon): Unit
  def insert(entityCreator: EC)(using DbCon): E
  def insertAll(entityCreators: Iterable[EC])(using DbCon): Vector[E]
  def insertReturning(entityCreator: EC)(using DbCon): E
  def insertAllReturning(entityCreators: Iterable[EC])(using DbCon): Vector[E]
  def update(entity: E)(using DbCon): Unit
  def updateAll(entities: Iterable[E])(using DbCon): Unit
```

Here's an example:

```scala
@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: Option[String],
  lastName: String,
  created: OffsetDateTime
) derives DbCodec

val userRepo = Repo[User, User, Long]

val countAfterUpdate = transact(ds):
  userRepo.deleteById(2L)
  userRepo.count
```

It is a best practice to encapsulate your SQL in repositories.

```scala
class UserRepo extends Repo[User, User, Long]
```

Also note that Repo extends ImmutableRepo. Some databases cannot support every method, and will throw UnsupportedOperationException.

### Database generated columns

It is often the case that database columns are auto-generated, for example, primary key IDs. This is why the Repo class has 3 type parameters. 

The first defines the Entity-Creator, which should omit any fields that are auto-generated. The entity-creator class must be an 'effective' subclass of the entity class, but it does not have to subclass the entity. This is verified at compile time.

The second type parameter is the Entity class, and the final is for the ID. If the Entity does not have a logical ID, use Null.

```scala
case class UserCreator(
  firstName: Option[String],
  lastName: String,
) derives DbCodec

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: Option[String],
  lastName: String,
  created: OffsetDateTime
) derives DbCodec

val userRepo = Repo[UserCreator, User, Long]

val newUser: User = transact(ds):
  userRepo.insertReturning(
    UserCreator(Some("Adam"), "Smith")
  )
```

### Specifications

Specifications help you write safe, dynamic queries.
An example use-case would be a search results page that allows users to sort and filter the paginated data.

1. If you need to perform joins to get the data needed, first create a database view.
2. Next, create an entity class that derives DbReader.
3. Finally, use the Spec class to create a specification.

Here's an example:

```scala
val partialName = "Ja"
val searchDate = OffsetDateTime.now.minusDays(2)
val idPosition = 42L

val spec = Spec[User]
  .where(sql"first_name ILIKE '$partialName%'")
  .where(sql"created >= $searchDate")
  .seek("id", SeekDir.Gt, idPosition, SortOrder.Asc)
  .limit(10)

val users: Vector[User] = userRepo.findAll(spec)
```

Note that both [seek pagination](https://blog.jooq.org/faster-sql-paging-with-jooq-using-the-seek-method/) and offset pagination is supported.

### Scala 3 Enum Support

Magnum supports Scala 3 enums (non-adt) fully, by default writing & reading them as Strings. For example,

```scala
@Table(PostgresDbType, SqlNameMapper.CamelToUpperSnakeCase)
enum Color derives DbCodec:
  case Red, Green, Blue

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: Option[String],
  lastName: String,
  created: OffsetDateTime,
  favoriteColor: Color
) derives DbCodec
```

### `DbCodec`: Typeclass for JDBC reading & writing

DbCodec is a Typeclass for JDBC reading & writing.

Built-in DbCodecs are provided for many types, including primitives, dates, Options, and Tuples. You can derive DbCodecs by adding `derives DbCodec` to your case class or enum.

```scala
val rs: ResultSet = ???
val ints: Vector[Int] = DbCodec[Int].read(rs)

val ps: PreparedStatement = ???
DbCodec[Int].writeSingle(22, ps)
```

### Defining your own DbCodecs

To modify the JDBC mappings, implement a given DbCodec instance as you would for any Typeclass.

## Motivation

Historically, database clients on the JVM fall into three categories.

* Object Oriented Repositories (Spring-Data, Hibernate)
* Functional DSLs (JOOQ, Slick, quill, zio-sql)
* SQL String interpolators (Anorm, doobie, plain jdbc)

Magnum is a Scala 3 library combining aspects of all three,
providing a typesafe and refactorable SQL interface,
which can express all SQL expressions, on all JDBC-supported databases.

Like in Zoolander (the movie), Magnum represents a 'new look' for Database access in Scala.

## Feature List

* Supports any database with a JDBC driver,
  including Postgres, MySql, Oracle, ClickHouse, H2, and Sqlite
* Efficient `sql" "` interpolator
* Purely-functional API
* Common queries (like insert, update, delete) generated at compile time
* Difficult to hit N+1 query problem
* Type-safe Transactions
* Supports database-generated columns
* Easy to use, Loom-ready API (no Futures or Effect Systems)
* Easy to define entities. Easy to implement DB support & codecs for custom types.
* Scales to complex SQL queries
* Specifications for building dynamic queries, such as table filters with pagination
* Supports high-performance [Seek pagination](https://blog.jooq.org/faster-sql-paging-with-jooq-using-the-seek-method/)
* Performant batch-queries

### Logging SQL queries

If you set the java.util Logging level to DEBUG, all SQL queries will be logged.
Setting to TRACE will log SQL queries and their parameters.

## Developing
The tests are written using TestContainers, which requires Docker be installed.

Table that compares frameworks:
* Refactoring-safe sql?
* Purely functional API?
* Supports nearly all databases?
* Common queries (like insert, update, delete) generated at compile time?
* Impossible to hit N+1 query problem?
* Type-safe transaction propagation?
* Supports generated columns?
* Loom-ready interface?
* Easy to define entities?
* Scales to complex SQL queries?
* Specifications for building dynamic queries?
* Performant Batch queries

## Todo
* Support MSSql
* Streaming support

## Talks and Presentations

* Scala Days 2023: [slides](/Magnum-Slides-to-Share.pdf)