package com.augustnagro.magnum.shared.impl

import com.augustnagro.magnum.shared.event.{SqlExceptionEvent, SqlSuccessEvent}

class SqlLogger:
  type F[+E, +A]
  def log(successEvent: SqlSuccessEvent): F[Nothing, Unit]
  def exceptionMsg(exceptionEvent: SqlExceptionEvent): F[Nothing, String]
