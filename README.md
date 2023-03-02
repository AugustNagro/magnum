## Magnum

A 'new look' for Repository-style Database access.


todo: try scala 3.3 rc
todo: delete DbConfig class
todo: testing of other databases: Oracle, MSSQL, DB2, SQLlite, Cassandra, Clickhouse, H2
todo: documentation
todo: DatabaseType type class that swaps out impl of ANY(*) queries
todo: sql.run method respect logging level and print query when appropriate.
todo: good error messages
todo: should update() and delete() methods return a value? (update could generate some keys.
  however, this breaks command-query-seperation.
  And could cause -y-warn-value-discard problems when unused)

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
