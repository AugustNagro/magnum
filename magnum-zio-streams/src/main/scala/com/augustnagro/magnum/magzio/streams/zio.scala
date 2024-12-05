package com.augustnagro.magnum.magzio.streams

import com.augustnagro.magnum.Query
import zio.stream.ZStream

extension [E](query: Query[e])
  def stream(fetchSize: Int = 0): ZStream

def stream(): Unit = ZStream.acquireReleaseWith(acquire = )
