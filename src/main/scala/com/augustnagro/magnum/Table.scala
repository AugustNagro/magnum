package com.augustnagro.magnum

import scala.annotation.StaticAnnotation

class Table(
    val dbType: DbType,
    val nameMapper: SqlNameMapper = SqlNameMapper.SameCase
) extends StaticAnnotation
