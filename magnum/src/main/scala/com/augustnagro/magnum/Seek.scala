package com.augustnagro.magnum

class Seek private[magnum] (
    val column: String,
    val seekDirection: SeekDir,
    val value: Any,
    val columnSort: SortOrder,
    val nullOrder: NullOrder,
    val codec: DbCodec[?]
)
