package com.augustnagro.magnum.common

export com.augustnagro.magnum.builders.{DbCon, DbTx, Transactor}
export com.augustnagro.magnum.codec.DbCodec
export com.augustnagro.magnum.repo.{
  Id,
  ImmutableRepo,
  Repo,
  RepoDefaults,
  SqlName,
  SqlNameMapper,
  Table
}
export com.augustnagro.magnum.spec.Spec
export com.augustnagro.magnum.tableinfo.TableInfo
export com.augustnagro.magnum.{connect, transact, sql}
