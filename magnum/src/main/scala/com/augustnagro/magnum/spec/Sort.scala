package com.augustnagro.magnum.spec

class Sort private[spec] (
    val column: String,
    val direction: SortOrder,
    val nullOrder: NullOrder
)
