package com.augustnagro.magnum.magzio.builders

import com.augustnagro.magnum.shared.impl.MagF
import zio.*

import scala.annotation.targetName

opaque type MIO[+E, +A] = IO[E, A]
opaque type MUIO[+A] = UIO[A]

object MIO:
  private[magzio] inline def apply[E, A](zio: IO[E, A]): MIO[E, A] =
    ZIO.succeed(zio)

  extension [E, A](mio: MIO[E, A])
//    infix def *>[E1 >: E, B](that: MIO[E1, B])(using Trace): MIO[E1, B] =
//      mio *> that
    private inline def myMio: MIO[E, A] = mio
    export myMio.{flatMap, *>}
//    def flatMap[E1 >: E, B](k: A => MIO[E1, B])(using Trace): MIO[E1, B] =
//      mio.flatMap(k)

  def succeed[A](a: => A)(using Trace): MUIO[A] = ZIO.succeed(a)
  def attempt[A](code: => A)(using Trace): MIO[Throwable, A] = ZIO.attempt(code)
  def logTrace(msg: => String)(using Trace): MUIO[Unit] = ZIO.logInfo(msg)
  def logDebug(msg: => String)(using Trace): MUIO[Unit] = ZIO.logDebug(msg)

  // todo make inline
  private[magzio] val MIOMagF = new MagF:
    override type F[+E, +A] = IO[E, A]
    override def logTrace(msg: String): UIO[Unit] = MIO.logTrace(msg)
    override def logDebug(msg: String): UIO[Unit] = MIO.logDebug(msg)
end MIO
