package com.augustnagro.magnum

enum DbType:
  /** For any database that supports enough of the ISO SQL specification to
    * implement all Repo methods
    */
  case SqlCompliant
  case MySql
  case H2
  case Oracle
  case Postgres
