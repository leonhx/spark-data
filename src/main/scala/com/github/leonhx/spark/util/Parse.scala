package com.github.leonhx.spark.util

import com.github.leonhx.spark.logging.logger

object Parse {
  def args(cmdArgs: Array[String]): Config =
    Config(cmdArgs.iterator.flatMap { arg =>
      if (arg.contains("=")) {
        val keyValue = arg.split("=")
        val key = keyValue.head
        val value = keyValue.tail.mkString("=")
        Some(key -> value)
      } else {
        logger.error(s"[Parse.args] ERROR_FORMAT: $arg")
        None
      }
    }.toSeq: _*)
}
