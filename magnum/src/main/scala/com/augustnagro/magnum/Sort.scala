package com.augustnagro.magnum

class Sort private[magnum] (
    val column: String,
    val direction: SortOrder,
    val nullOrder: NullOrder
)
