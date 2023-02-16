package com.augustnagro.magnum

class ArrayProduct(arr: Array[Any]) extends Product:
  override def productArity: Int = arr.length

  override def productElement(n: Int): Any = arr(n)

  override def productIterator: Iterator[Any] = arr.iterator

  override def productPrefix: String = "ArrayProduct"

  override def canEqual(that: Any): Boolean = that match
    case ap: ArrayProduct if ap.productArity == productArity => true
    case _                                                   => false
