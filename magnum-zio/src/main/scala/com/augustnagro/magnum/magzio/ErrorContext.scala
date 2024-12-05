package com.augustnagro.magnum.magzio

import com.augustnagro.magnum.{Transactor as _, *}

import scala.util.control.NoStackTrace

private class Break[E](val value: E) extends NoStackTrace

/** Abort this SQL Transaction or Connection and become a failed ZIO */
def fail[E](error: E)(using DbCon): Nothing = throw Break(error)
