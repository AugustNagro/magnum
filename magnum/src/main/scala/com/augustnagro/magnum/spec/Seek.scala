package com.augustnagro.magnum.spec

import com.augustnagro.magnum.codec.DbCodec

class Seek private[spec] (
    val column: String,
    val seekDirection: SeekDir,
    val value: Any,
    val columnSort: SortOrder,
    val nullOrder: NullOrder,
    val codec: DbCodec[?]
)
