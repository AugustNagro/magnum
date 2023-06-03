package com.augustnagro.magnum

private class ProductIterable(product: Product) extends Iterable[Any]:
  override def iterator: Iterator[Any] = product.productIterator

  override def toString(): String =
    iterator.mkString("\n(", ",\n", ")\n")
