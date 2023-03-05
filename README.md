## Magnum

A 'new look' for Repository-style Database access.

Supports any database with a JDBC driver, including:
* Postgres
* MySql
* H2
* Oracle

todo: Factory db support
todo: testing of other databases: Oracle, MSSQL, DB2, SQLlite, Cassandra, Clickhouse, H2
todo: documentation
todo: sql.run method respect logging level and print query when appropriate.
todo: good error messages

Todo: Comparison with existing frameworks:

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
