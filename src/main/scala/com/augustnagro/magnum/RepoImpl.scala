package com.augustnagro.magnum

trait RepoImpl[EC, E, ID]:
  def tableWithAlias: String
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
  def update(entity: E)(using DbCon): Unit
  def updateAll(entities: Iterable[E])(using DbCon): Unit
