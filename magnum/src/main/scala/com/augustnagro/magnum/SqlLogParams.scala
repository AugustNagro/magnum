package com.augustnagro.magnum

/** The parameter shape used by SQL logging. */
private[magnum] enum SqlLogParams:
  case Fragment(params: Seq[Any])
  case Single(param: Any)
  case Batch(params: Iterable[?])

  /** Returns a fresh iterator so an event can be rendered more than once. */
  def iterator: Iterator[Iterator[Any]] =
    this match
      case Fragment(params) =>
        params match
          // A single Product is one interpolated parameter whose fields are
          // bound separately; multiple fragment parameters form one row.
          case Seq(param: Product) => Iterator(param.productIterator)
          case _                   => Iterator(params.iterator)
      case Single(param: Product) => Iterator(param.productIterator)
      case Single(param)          => Iterator(Iterator.single(param))
      case Batch(params) =>
        params.iterator.map:
          case param: Product => param.productIterator
          case param          => Iterator.single(param)

end SqlLogParams
