package com.augustnagro.magnum.batch

import scala.util.boundary

/** The total number of rows updated, or SuccessNoInfo if unknown. */
enum BatchUpdateResult:
  case Success(rowsUpdated: Long)
  case SuccessNoInfo
