package com.augustnagro.magnum

private[magnum] enum SqlLogParams:
  case Fragment(params: Seq[Any])
  case Single(param: Any)
  case Batch(params: Iterable[?])

  def iterator: Iterator[Iterator[Any]] =
    this match
      case Fragment(params) =>
        params match
          case Seq(param: Product) => Iterator(param.productIterator)
          case _                   => Iterator(params.iterator)
      case Single(param: Product) => Iterator(param.productIterator)
      case Single(param)          => Iterator(Iterator.single(param))
      case Batch(params)          =>
        params.iterator.map:
          case param: Product => param.productIterator
          case param          => Iterator.single(param)
