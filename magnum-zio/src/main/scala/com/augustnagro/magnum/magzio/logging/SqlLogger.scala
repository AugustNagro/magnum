package com.augustnagro.magnum.magzio.logging

import com.augustnagro.magnum.shared.event.{SqlExceptionEvent, SqlSuccessEvent}
import com.augustnagro.magnum.magzio.builders.MIO

/** Trait to provide logging of Magnum SQL statements.
  */
trait SqlLogger:
  /** Log a successful SQL statement execution. If a query fails this logger
    * will not be triggered.
    */
  def log(successEvent: SqlSuccessEvent): MIO[Nothing, Unit]
  
  def exceptionMsg(exceptionEvent: SqlExceptionEvent): MIO[Nothing, String]
