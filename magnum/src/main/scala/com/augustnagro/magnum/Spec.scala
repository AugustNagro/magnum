package com.augustnagro.magnum

import java.util.StringJoiner

class Spec[E] private (
    val prefix: Option[Frag],
    val predicates: Vector[Frag],
    val limit: Option[Int],
    val offset: Option[Long],
    val sorts: Vector[Sort],
    val seeks: Vector[Seek]
):

  def prefix(sql: Frag): Spec[E] =
    new Spec(Some(sql), predicates, limit, offset, sorts, seeks)

  def where(sql: Frag): Spec[E] =
    new Spec(prefix, predicates :+ sql, limit, offset, sorts, seeks)

  def orderBy(
      column: String,
      direction: SortOrder = SortOrder.Default,
      nullOrder: NullOrder = NullOrder.Default
  ): Spec[E] =
    val sort = Sort(column, direction, nullOrder)
    new Spec(prefix, predicates, limit, offset, sorts :+ sort, seeks)

  def limit(limit: Int): Spec[E] =
    new Spec(prefix, predicates, Some(limit), offset, sorts, seeks)

  def offset(offset: Long): Spec[E] =
    new Spec(prefix, predicates, limit, Some(offset), sorts, seeks)

  def seek[V](
      column: String,
      seekDirection: SeekDir,
      value: V,
      columnSort: SortOrder,
      nullOrder: NullOrder = NullOrder.Default
  )(using codec: DbCodec[V]): Spec[E] =
    val seek = Seek(column, seekDirection, value, columnSort, nullOrder, codec)
    new Spec(prefix, predicates, limit, offset, sorts, seeks :+ seek)

end Spec

object Spec:
  def apply[E]: Spec[E] =
    new Spec(None, Vector.empty, None, None, Vector.empty, Vector.empty)
