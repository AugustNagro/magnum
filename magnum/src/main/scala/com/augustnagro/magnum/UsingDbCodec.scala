package com.augustnagro.magnum

import scala.annotation.StaticAnnotation

/** Selects the codec for one field of a case class that derives `DbCodec`. This
  * is useful when fields of the same type need different database mappings.
  *
  * {{{
  *   @Table(PostgresDbType)
  *   case class Event(
  *     @UsingDbCodec(codecA) first: Long,
  *     @UsingDbCodec(codecB) second: Long
  *   ) derives DbCodec
  * }}}
  */
class UsingDbCodec[A](val codec: DbCodec[A]) extends StaticAnnotation
