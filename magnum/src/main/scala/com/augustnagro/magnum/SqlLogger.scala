package com.augustnagro.magnum

import java.lang.System.Logger.Level
import scala.concurrent.duration.FiniteDuration

/** Trait to provide logging of Magnum SQL statements.
  */
trait SqlLogger:
  /** Log a successful SQL statement execution. If a query fails a
    * [[SqlException]] will be thrown, and this logger will not be triggered.
    */
  def log(logEvent: SqlLogEvent): Unit

  /** Constructs the exception message for [[SqlException]]s */
  def exceptionMsg(exceptionEvent: SqlExceptionEvent): String

object SqlLogger:
  object NoOp extends SqlLogger:
    override def log(logEvent: SqlLogEvent): Unit = ()
    override def exceptionMsg(exceptionEvent: SqlExceptionEvent): String =
      exceptionEvent.cause.getMessage

  object Default extends SqlLogger:
    override def log(logEvent: SqlLogEvent): Unit =
      if Log.isLoggable(Level.TRACE) then
        Log.log(
          Level.TRACE,
          s"""Executed Query in ${logEvent.execTime}:
             |${logEvent.sql}
             |
             |With values:
             |${paramsString(logEvent.params)}""".stripMargin
        )
      else if Log.isLoggable(Level.DEBUG) then
        Log.log(
          Level.DEBUG,
          s"""Executed Query in ${logEvent.execTime}:
             |${logEvent.sql}""".stripMargin
        )

    override def exceptionMsg(exceptionEvent: SqlExceptionEvent): String =
      if Log.isLoggable(System.Logger.Level.TRACE) then
        s"""Error executing query:
             |${exceptionEvent.sql}
             |With message:
             |${exceptionEvent.cause.getMessage}
             |And values:
             |${paramsString(exceptionEvent.params)}
             |""".stripMargin
      else s"""Error executing query:
           |${exceptionEvent.sql}
           |With message:
           |${exceptionEvent.cause}
           |""".stripMargin
  end Default

  def logSlowQueries(slowerThan: FiniteDuration): SqlLogger = new:
    override def log(logEvent: SqlLogEvent): Unit =
      if logEvent.execTime > slowerThan then
        if Log.isLoggable(Level.TRACE) then
          Log.log(
            Level.WARNING,
            s"""Executed SLOW Query in ${logEvent.execTime}:
             |${logEvent.sql}
             |
             |With values:
             |${paramsString(logEvent.params)}""".stripMargin
          )
        else if Log.isLoggable(Level.WARNING) then
          Log.log(
            Level.WARNING,
            s"""Executed SLOW Query in ${logEvent.execTime}:
               |${logEvent.sql}""".stripMargin
          )
        end if
      else Default.log(logEvent)

    override def exceptionMsg(exceptionEvent: SqlExceptionEvent): String =
      Default.exceptionMsg(exceptionEvent)
end SqlLogger
