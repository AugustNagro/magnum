package com.augustnagro.magnum

private case class Sort(
    column: DbSchemaName,
    direction: SortOrder,
    nullOrder: NullOrder
)
