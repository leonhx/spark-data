package com.github.leonhx.spark

import com.dianping.midas.ranker.common4s.util.DateTimeUtil
import com.github.leonhx.spark.logging.LogLevel.LogLevel
import com.github.leonhx.spark.util.SparkUtil
import org.joda.time.DateTime

package object logging {

  object logger extends Serializable {
    @transient private var _log: Logger = _
    private var _logRootPath: String = _

    def setLogRoot(path: String): Unit = _logRootPath = path

    def setLogLevel(logLevel: LogLevel): Unit =
      if (_log == null) {
        _log = Logger(logLevel)
      } else {
        _log = Logger(logLevel, _log.saveLevel)
      }

    def setLogLevel(level: String): Unit = setLogLevel(LogLevel.of(level))

    def setSaveLevel(logLevel: LogLevel): Unit =
      if (_log == null) {
        _log = Logger(LogLevel.Error, logLevel)
      } else {
        _log = Logger(_log.level, logLevel)
      }

    def setSaveLevel(level: String): Unit = setSaveLevel(LogLevel.of(level))

    private def safe(action: => Unit): Unit = if (_log != null) action

    def debug(msg: => String): Unit = safe(_log.debug(msg))

    def debug(msg: => String, e: Throwable): Unit = safe(_log.debug(msg, e))

    def info(msg: => String): Unit = safe(_log.info(msg))

    def info(msg: => String, e: Throwable): Unit = safe(_log.info(msg, e))

    def warn(msg: => String): Unit = safe(_log.warn(msg))

    def warn(msg: => String, e: Throwable): Unit = safe(_log.warn(msg, e))

    def error(msg: => String): Unit = safe(_log.error(msg))

    def error(msg: => String, e: Throwable): Unit = safe(_log.error(msg, e))

    def isDebugEnabled: AnyVal = safe(_log.isDebugEnabled)

    def isInfoEnabled: AnyVal = safe(_log.isInfoEnabled)

    def isErrorEnabled: AnyVal = safe(_log.isErrorEnabled)

    def save(fileName: String, date: DateTime): Unit =
      if (_logRootPath != null && _logRootPath.nonEmpty && fileName.nonEmpty) {
        val sparkSession = SparkUtil.session
        val dtKey = DateTimeUtil.formatDateKey(date)
        val appId = sparkSession.sparkContext.applicationId
        val outPath = s"${_logRootPath}/$fileName/dt=$dtKey/app=$appId"
        val subject = s"$fileName $dtKey"
        safe(_log.save(outPath, subject))
      }
  }

}
