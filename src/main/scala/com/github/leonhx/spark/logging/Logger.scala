package com.github.leonhx.spark.logging

import com.github.leonhx.spark.logging.LogLevel.LogLevel
import com.github.leonhx.spark.util.SparkUtil
import com.meituan.huangxin09.spark.logging.LogLevel.LogLevel
import com.meituan.huangxin09.spark.util.SparkUtil
import org.apache.commons.mail.SimpleEmail
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scala.compat.Platform._

private[logging]
case class Logger(level: LogLevel, saveLevel: LogLevel = LogLevel.Warn)
  extends Serializable {
  private val log = ListBuffer[String]()

  val isDebugEnabled: Boolean = LogLevel.order(level) <= LogLevel.order(LogLevel.Debug)

  val isInfoEnabled: Boolean = LogLevel.order(level) <= LogLevel.order(LogLevel.Info)

  val isErrorEnabled: Boolean = LogLevel.order(level) <= LogLevel.order(LogLevel.Error)

  def debug(message: => String): Unit = logToStdOut(LogLevel.Debug, message)

  def debug(message: => String, e: Throwable): Unit = logToStdOut(LogLevel.Debug, message, e)

  def info(message: => String): Unit = logToStdOut(LogLevel.Info, message)

  def info(message: => String, e: Throwable): Unit = logToStdOut(LogLevel.Info, message, e)

  def warn(message: => String): Unit = logToStdOut(LogLevel.Warn, message)

  def warn(message: => String, e: Throwable): Unit = logToStdOut(LogLevel.Warn, message, e)

  def error(message: => String): Unit = logToStdOut(LogLevel.Error, message)

  def error(message: => String, e: Throwable): Unit = logToStdOut(LogLevel.Error, message, e)

  private def logToStdOut(level: LogLevel, message: => String): Unit =
    if (LogLevel.order(this.level) <= LogLevel.order(level)) {
      val logString = s"[${LogLevel.toString(level)}] ${new DateTime()} ${message.trim}"
      println(logString)
      if (LogLevel.order(this.saveLevel) <= LogLevel.order(level)) log += logString
    }

  private def logToStdOut(level: LogLevel, message: => String, e: Throwable): Unit =
    if (LogLevel.order(this.level) <= LogLevel.order(level)) {
      val exceptionInfo = (e.getMessage +: e.getStackTrace).mkString("", EOL, EOL)
      val logString = s"[${LogLevel.toString(level)}] ${new DateTime()} ${message.trim}\n$exceptionInfo"
      println(logString)
      if (LogLevel.order(this.saveLevel) <= LogLevel.order(level)) log += logString
    }

  def save(outPath: String, subject: String): Unit =
    if (outPath.nonEmpty && log.nonEmpty) {
      val sparkSession = SparkUtil.session
      import sparkSession.implicits._

      sparkSession.createDataset(log).repartition(1)
        .write.text(SparkUtil.generatePathLike(s"$outPath/version=%d"))

      // TODO:
      val email = new SimpleEmail()
      email.setHostName("<TO BE FILL IN>")
      email.setSmtpPort(25)
      email.setFrom("<TO BE FILL IN>")

      email.setSubject(s"[Spark Job Finished] $subject")
      email.setMsg(s"Log Path: $outPath\n\n${log.mkString("\n")}")
      email.addTo("<TO BE FILL IN>")
      email.send()
    }
}
