package com.augustnagro.magnum

import scala.collection.*

// template for all renderables

abstract class Renderables[+SELF <: IndexedSeq[String]](
    private val strs: IArray[String],
    private val factory: SpecificIterableFactory[String, SELF]
) extends IndexedSeq[String],
      immutable.IndexedSeqOps[String, IndexedSeq, SELF],
      immutable.StrictOptimizedSeqOps[String, IndexedSeq, SELF]:
  self: SELF =>

  def length: Int = strs.length

  def apply(idx: Int): String =
    if idx < 0 || length <= idx then throw new IndexOutOfBoundsException
    strs(idx)

  // Mandatory overrides of `fromSpecific`, `newSpecificBuilder`,
  // and `empty`, from `IterableOps`
  override protected def fromSpecific(coll: IterableOnce[String]): SELF =
    factory.fromSpecific(coll)
  override protected def newSpecificBuilder: mutable.Builder[String, SELF] =
    factory.newBuilder
  override def empty: SELF = factory.empty

  // Overloading of `appended`, `prepended`, `appendedAll`, `prependedAll`,
  // `map`, `flatMap` and `concat` to return a `Columns` when possible
  def concat(suffix: IterableOnce[String]): SELF =
    strictOptimizedConcat(suffix, newSpecificBuilder)
  inline final def ++(suffix: IterableOnce[String]): SELF = concat(suffix)
  def appended(String: String): SELF =
    (newSpecificBuilder ++= this += String).result()
  def appendedAll(suffix: Iterable[String]): SELF =
    strictOptimizedConcat(suffix, newSpecificBuilder)
  def prepended(String: String): SELF =
    (newSpecificBuilder += String ++= this).result()
  def prependedAll(prefix: Iterable[String]): SELF =
    (newSpecificBuilder ++= prefix ++= this).result()
  def map(f: String => String): SELF =
    strictOptimizedMap(newSpecificBuilder, f)
  def flatMap(f: String => IterableOnce[String]): SELF =
    strictOptimizedFlatMap(newSpecificBuilder, f)

  override def iterator: Iterator[String] = new AbstractIterator[String]:
    private var current = 0
    def hasNext = current < self.length
    def next(): String =
      val elem = self(current)
      current += 1
      elem

// concrete classes

class AllColumns(private val cols: IArray[String])
    extends Renderables[AllColumns](cols, AllColumns):
  override def className = "Columns"
  override def toString(): String = mkString(", ")

class InsertColumns(private val cols: IArray[String])
    extends Renderables[InsertColumns](cols, InsertColumns):
  override def className = "InsertColumns"
  override def toString(): String = mkString("(", ", ", ")")

// Factories

class RenderablesFactory[A <: Renderables[A]](
    creator: IterableOnce[String] => A
) extends SpecificIterableFactory[String, A]:

  def fromSeq(buf: collection.Seq[String]): A =
    creator(buf)
  def empty: A = fromSeq(Seq.empty)

  def newBuilder: mutable.Builder[String, A] =
    mutable.ArrayBuffer.newBuilder[String].mapResult(fromSeq)

  def fromSpecific(it: IterableOnce[String]): A = it match
    case seq: collection.Seq[String] => fromSeq(seq)
    case _                           => creator(it)

object AllColumns
    extends RenderablesFactory[AllColumns](it =>
      new AllColumns(IArray.from(it))
    )

object InsertColumns
    extends RenderablesFactory[InsertColumns](it =>
      new InsertColumns(IArray.from(it))
    )
