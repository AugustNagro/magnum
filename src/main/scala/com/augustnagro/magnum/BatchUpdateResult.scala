package com.augustnagro.magnum

import scala.util.boundary

/** The total number of rows updated, or SuccessNoInfo if unknown. */
enum BatchUpdateResult:
  case Success(rowsUpdated: Long)
  case SuccessNoInfo
