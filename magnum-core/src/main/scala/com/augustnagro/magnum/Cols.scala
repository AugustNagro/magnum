package com.augustnagro.magnum

class Cols(dbTableName: String, nameMapper: SqlNameMapper) extends Selectable:
  def selectDynamic(fieldName: String): Any =
    Col(dbTableName + "." + nameMapper.toColumnName(fieldName))
