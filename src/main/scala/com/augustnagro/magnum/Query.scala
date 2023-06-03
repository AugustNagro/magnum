package com.augustnagro.magnum

case class Query[E](frag: Frag, codec: DbCodec[E]):

  def run(using )
