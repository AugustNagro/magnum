package com.augustnagro.magnum

private case class Sort(
    column: String,
    direction: SortOrder,
    nullOrder: NullOrder
)
