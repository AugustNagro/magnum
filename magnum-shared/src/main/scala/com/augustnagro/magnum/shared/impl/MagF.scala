package com.augustnagro.magnum.shared.impl

/** Typeclass with the capabilities magnum requires for an effect type F */
trait MagF:
  type F[+E, +A]
  def logTrace(msg: String): F[Nothing, Unit]
  def logDebug(msg: String): F[Nothing, Unit]
