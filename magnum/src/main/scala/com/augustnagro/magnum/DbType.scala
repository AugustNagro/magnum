package com.augustnagro.magnum

enum DbType:
  /** For any database that supports enough of the ISO SQL specification to
    * implement Repo methods
    */
  case SqlCompliant
  case MySql
