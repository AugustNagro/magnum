package com.augustnagro.magnum.repo

import com.augustnagro.magnum.dbtype.DbType
import com.augustnagro.magnum.repo.SqlNameMapper

import scala.annotation.StaticAnnotation

class Table(
    val dbType: DbType,
    val nameMapper: SqlNameMapper = SqlNameMapper.SameCase
) extends StaticAnnotation
