package com.augustnagro.magnum.shared.event

private def parseParams(params: Any): Iterator[Iterator[Any]] =
  params match
    case p: Product => Iterator(p.productIterator)
    case it: Iterable[?] =>
      it.headOption match
        case Some(h: Product) =>
          it.asInstanceOf[Iterable[Product]]
            .iterator
            .map(_.productIterator)
        case _ =>
          Iterator(it.iterator)
    case x => Iterator(Iterator(x))
