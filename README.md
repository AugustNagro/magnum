## Magnum

[![Latest version](https://index.scala-lang.org/augustnagro/magnum/magnum/latest.svg?color=orange)](https://index.scala-lang.org/augustnagro/magnum/magnum)

Yet another database client for Scala. No dependencies, high productivity.

* [Installing](#installing)
* [ScalaDoc](#scaladoc)
* [Documentation](#documentation)
  * [`connect` creates a database connection](#connect-creates-a-database-connection)
  * [`transact` creates a database transaction](#transact-creates-a-database-transaction)
  * [Type-safe Transaction & Connection Management](#type-safe-transaction--connection-management)
  * [Customizing Transactions](#customizing-transactions)
  * [Sql Interpolator, Frag, Query, Update, Returning](#sql-interpolator-frag-query-and-update)
  * [Batch Updates](#batch-updates)
  * [Immutable Repositories](#immutable-repositories)
  * [Repositories](#repositories)
  * [Database generated columns](#database-generated-columns)
  * [Specifications](#specifications)
  * [Scala 3 Enum & NewType Support](#scala-3-enum--newtype-support)
  * [`DbCodec`: Typeclass for JDBC reading & writing](#dbcodec-typeclass-for-jdbc-reading--writing)
  * [Future-Proof Queries](#future-proof-queries)
  * [Splicing Literal Values into Frags](#splicing-literal-values-into-frags)
  * [Postgres Module](/PG-MODULE.md)
  * [Logging](#logging-sql-queries)
* [Integrations](#integrations)
  * [ZIO](#zio) 
* [Motivation](#motivation)
* [Feature List And Database Support](#feature-list)
* [Talks and Blogs](#talks-and-blogs)
* [Frequently Asked Questions](#frequently-asked-questions)

## Installing

```
"com.augustnagro" %% "magnum" % "2.0.0"
```

Magnum requires Scala >= 3.3.0

You must also install the JDBC driver for your database, for example:

```
"org.postgresql" % "postgresql" % "<version>"
```

And for performance, a JDBC connection pool like [HikariCP](https://github.com/brettwooldridge/HikariCP)

## ScalaDoc

https://javadoc.io/doc/com.augustnagro/magnum_3

## Documentation

### `connect` creates a database connection.

```scala
import com.augustnagro.magnum.common.*

val xa = Transactor(dataSource: javax.sql.DataSource)

val users: Vector[User] = xa.connect:
  sql"SELECT * FROM user".query[User].run()
```

The `connect` method accepts a context function of type `DbCon ?=> A` (essentially `implicit DbCon => A` in Scala 2).

### `transact` creates a database transaction.

Like `connect`, `transact` accepts a context function.
The context function provides a `DbTx` capability.
If the function throws, the transaction will be rolled back.

```scala
// update is rolled back
xa.transact:
  sql"UPDATE user SET first_name = $firstName WHERE id = $id".update.run()
  thisMethodThrows()
```

### Type-safe Transaction & Connection Management

Annotate transactional methods with `using DbTx`, and ones that require connections with `using DbCon`.

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

### Customizing transactions

`Transactor` lets you customize the transaction (or connection) behavior.

```scala
val xa = Transactor(
  dataSource = ???,
  sqlLogger = SqlLogger.logSlowQueries(500.milliseconds),
  connectionConfig = con =>
    con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
)

xa.transact:
  sql"SELECT id from myUser".query[Long].run()
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

Or an update with a `RETURNING` clause via `returning`:

```scala
val updateReturning: Returning =
  sql"""
     UPDATE user SET first_name = 'Buddha'
     WHERE last_name = 'Harper'
     RETURNING id
     """.returning[Long]
```

All are executed via `run()(using DbCon)`:

```scala
xa.transact:
  val tuples: Vector[(Long, String)] = query.run()
  val updatedRows: Int = update.run()
  val updatedIds: Vector[Long] = updateReturning.run()
```

### Batch Updates

Batch updates are supported via `batchUpdate` method in package `com.augustnagro.magnum`.

```scala
xa.connect:
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

xa.transact:
  val cnt = userRepo.count
  val userOpt = userRepo.findById(2L)
```

Importantly, class User is annotated with `@Table`, which defines the table's database type. The annotation optionally specifies the name-mapping between scala fields and column names. You can also use the `@SqlName` annotation on individual fields. Finally, The table must `derive DbCodec`, or otherwise provide an implicit DbCodec instance.

The optional `@Id` annotation denotes the table's primary key. Not setting `@Id` will default to using the first field. If there is no logical id, then remove the annotation and use Null in the ID type parameter of Repositories (see next).

You can choose to use composition or inheritance to encapsulate your SQL in repositories (Scala 3 [exports clauses](https://docs.scala-lang.org/scala3/reference/other-new-features/export.html) are a hidden gem):

```scala
object UserRepo extends ImmutableRepo[User, Long]:
  def firstNamesForLast(lastName: String)(using DbCon): Vector[String] =
    sql"""
      SELECT DISTINCT first_name
      FROM user
      WHERE last_name = $lastName
      """.query[String].run()
      
// alternatively
object UserSql:
  private val userRepo = ImmutableRepo[User, Long]

  export userRepo.{findById, count}

  def firstNamesForLast(lastName: String)(using DbCon): Vector[String] =
    sql"""
      SELECT DISTINCT first_name
      FROM user
      WHERE last_name = $lastName
      """.query[String].run()
```

### Repositories

The `Repo` class extends ImmutableRepo and also defines:

```scala
  def delete(entity: E)(using DbCon): Unit
  def deleteById(id: ID)(using DbCon): Unit
  def truncate()(using DbCon): Unit
  def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult
  def deleteAllById(ids: Iterable[ID])(using DbCon): BatchUpdateResult
  def insert(entityCreator: EC)(using DbCon): Unit
  def insertAll(entityCreators: Iterable[EC])(using DbCon): Unit
  def insertReturning(entityCreator: EC)(using DbCon): E
  def insertAllReturning(entityCreators: Iterable[EC])(using DbCon): Vector[E]
  def update(entity: E)(using DbCon): Unit
  def updateAll(entities: Iterable[E])(using DbCon): BatchUpdateResult
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

val countAfterUpdate = xa.transact:
  userRepo.deleteById(2L)
  userRepo.count
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

val newUser: User = xa.transact:
  userRepo.insertReturning(
    UserCreator(Some("Adam"), "Smith")
  )
```

### Specifications

Specifications help you write safe, dynamic queries.
An example use-case would be a search results page that allows users to sort and filter the paginated data.

Here's an example:

```scala
val spec = Spec[User]
  .where(sql"first_name ILIKE '$partialName%'")
  .where(lastNameOpt.map(ln => sql"last_name = $ln").getOrElse(sql""))
  .where(sql"created >= $searchDate")
  .seek("id", SeekDir.Gt, idPosition, SortOrder.Asc)
  .limit(10)

val users: Vector[User] = userRepo.findAll(spec)

def partialName = "Ja"
def lastNameOpt = Option("Brown")
def searchDate = OffsetDateTime.now.minusDays(2)
def idPosition = 42L
```

Note that both [seek pagination](https://blog.jooq.org/faster-sql-paging-with-jooq-using-the-seek-method/) and offset pagination is supported. If you need to use joins to select the columns, use the `Spec.prefix` method.

### Scala 3 Enum & NewType Support

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

For Postgres Enum types, see the [Postgres Module Docs](/PG-MODULE.md)

NewTypes and Opaque Type Alias can cause issues with derivation since given DbCodecs are not available. A simple way to provide them is using DbCodec.bimap:

```scala
opaque type MyId = Long

object MyId:
  def apply(id: Long): MyId =
    require(id >= 0)
    id

  extension (myId: MyId)
    def underlying: Long = myId

  given DbCodec[MyId] =
    DbCodec[Long].biMap(MyId.apply, _.underlying)

xa.transact:
  val id = MyId(123L)
  sql"UPDATE my_table SET x = true WHERE id = $id".update.run()
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

To modify the JDBC mappings, implement a given DbCodec instance as you would for any Typeclass. You can also use DbCodec.biMap on an existing codec.

### Future-Proof Queries

A common problem when writing SQL queries is that they're difficult to refactor. When a column or table name changes you have to do a global find & replace. And if you miss a query, it's discovered at runtime.

There's also lots of repetition when writing SQL. Magnum's repositories help scrap the boilerplate, but writing `SELECT a, b, c, d, ...` for a large table quickly gets tiring.

To help with this, Magnum offers a `TableInfo` class to enable 'future-proof' queries. An important caveat is that these queries are harder to copy/paste into SQL editors like PgAdmin or DbBeaver (of course, you can still find them in [DEBUG logs](#logging-sql-queries))

Here's some examples:

```scala
import com.augustnagro.magnum.common.*

case class UserCreator(firstName: String, age: Int) derives DbCodec

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class User(id: Long, firstName: String, age: Int) derives DbCodec

object UserSql:
  private val u = TableInfo[UserCreator, User, Long]

  def allUsers(using DbCon): Vector[User] =
    // equiv to 
    // SELECT id, first_name, age FROM user
    sql"SELECT ${u.all} FROM $u".query[User].run()

  def firstNamesForLast(lastName: String)(using DbCon): Vector[String] =
    // equiv to
    // SELECT DISTINCT first_name FROM user WHERE last_name = ?
    sql"""
       SELECT DISTINCT ${u.firstName} FROM $u
       WHERE ${u.lastName} = $lastName
       """.query[String].run()

  def insertOrIgnore(creator: UserCreator)(using DbCon): Unit =
    // equiv to
    // INSERT OR IGNORE INTO user (first_name, age) VALUES (?, ?)
    sql"INSERT OR IGNORE INTO $u ${u.insertCols} VALUES ($creator)".update.run()
```

It's important that `val Table = TableInfo[X, Y, Z]` is not explicitly typed, otherwise its structural typing will be destroyed.

In the case of multiple joins, you can use `TableInfo.alias(String)` to prevent name conflicts:

```scala
val c = TableInfo[Car].alias("c")
val p = TableInfo[Person].alias("p")

sql"""
   SELECT ${c.all}, ${p.firstName}
   FROM $c
   JOIN $p ON ${p.id} = ${c.personId}
   """.query.run()
```

### Splicing Literal Values into Frags

To splice Strings directly into `sql` statements, you can interpolate `SqlLiteral` values. For example,

```scala
val table = SqlLiteral("beans")
  
sql"select * from $table"
```

This feature should be used sparingly and never with untrusted input. 

### Logging SQL queries

If you set the java.util Logging level to DEBUG, all SQL queries will be logged.
Setting to TRACE will log SQL queries and their parameters.

#### Logging Slow Queries

You can log slow queries by using the `Transactor` class in conjunction with `SqlLogger.logSlowQueries(FiniteDuration)`. See [Customizing Transactions](#customizing-transactions) for an example. You can also implement your own SqlLogger subclass as desired.

## Integrations

### ZIO

Magnum provides a fine layer of integration with ZIO.    
The `magnum-zio` module provides an implementation of the `connect` and `transact` utils that return a ZIO effect.

To use the ZIO integration, add the following dependency:
```scala
"com.augustnagro" %% "magnum-zio" % "x.x.x"
```

and import these utils in your code with:
```scala
import com.augustnagro.magnum.magzio.*
```

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
* Difficult to hit [N+1 query problem](https://stackoverflow.com/questions/97197/what-is-the-n1-selects-problem-in-orm-object-relational-mapping)
* Type-safe Transactions
* Supports database-generated columns
* Easy to use, Loom-ready API (no Futures or Effect Systems)
* Easy to define entities. Easy to implement DB support & codecs for custom types.
* Scales to complex SQL queries
* Specifications for building dynamic queries, such as table filters with pagination
* Supports high-performance [Seek pagination](https://blog.jooq.org/faster-sql-paging-with-jooq-using-the-seek-method/)
* Performant batch-queries

## Developing
The tests are written using TestContainers, which requires Docker be installed.

## Talks and Blogs

* Scala Days 2023: [slides](/Magnum-Slides-to-Share.pdf), [talk](https://www.youtube.com/watch?v=iKNRS5b1zAY)
* Functional Scala 2024: https://www.youtube.com/watch?v=pkBfdHkeTtA

## Frequently Asked Questions

#### Does Magnum support nested entities like:

```scala
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class Company(
  name: String,
  address: Address,
  ) derives DbCodec

case class Address(
  street: String,
  city: String,
  zipCode: String,
  country: String
) derives DbCodec
```

NO; Magnum only supports deriving flat entity class structures. This keeps things simple and makes it obvious how the Scala entity class maps to the SQL table.

We may add support for SQL UDTs (user defined types) in the future; however at the time of writing, UDTs are not well-supported by JDBC drivers.

You could also express the above example using a foreign key to an Address table, like so:

```scala
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class Company(
  name: String,
  addressId: AddressId,
) derives DbCodec

opaque type AddressId = Long
object AddressId:
  def apply(id: Long): AddressId = id
  extension (id: AddressId)
    def underlying: Long = id
  given DbCodec[AddressId] =
    DbCodec[Long].biMap(AddressId.apply, _.underlying)

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class Address(
  @Id id: AddressId,
  street: String,
  city: String,
  zipCode: String,
  country: String
) derives DbCodec

def companyInfo(companyName: String)(using DbCon): Vector[(Company, Address)] =
  val c = TableInfo[Company, Company, String].alias("c")
  val a = TableInfo[Address, Address, Long].alias("a")
  sql"""
     SELECT ${c.all}, ${a.all}"
     FROM $c
     JOIN $a ON ${a.id} = ${c.addressId}
     WHERE ${c.name} = $companyName
     """.query[(Company, Address)].run()
```

#### UUID DbCodec doesn't work for my database

Some databases directly support the UUID type; these include Postgres, Clickhouse, and H2. When using the built-in `DbCodec[UUID]`, defined in `DbCodec.scala`, serialization and deserialization of `java.util.UUID` will work as expected.

Other databases like MySql, Oracle, and Sqlite, however, do not natively support UUID columns. Users have to choose an alternate datatype to store the UUID: most commonly `varchar(36)` or `binary(16)`. The JDBC drivers for these databases do not support direct serialization and deserialization of `java.util.UUID`, therefore the default `DbCodec[UUID]` will not be sufficient. Instead, import the appropriate codec from `com.augustnagro.magnum.UUIDCodec`. For example,

```scala
import com.augustnagro.magnum.common.*
import com.augustnagro.magnum.UUIDCodec.VarCharUUIDCodec
import java.util.UUID

@Table(MySqlDbType)
case class Person(@Id id: Long, name: String, tracking_id: Option[UUID]) derives DbCodec
```

## Todo
* Support MSSql
* Cats Effect & ZIO modules
