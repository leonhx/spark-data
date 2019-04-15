package com.github.leonhx.spark.logging

object LogLevel extends Enumeration {
  type LogLevel = Value

  val Debug, Info, Warn, Error, None = Value

  def toString(level: LogLevel): String = level match {
    case Debug => "DEBUG"
    case Info => "INFO"
    case Warn => "WARN"
    case Error => "ERROR"
    case None => "NONE"
  }

  def order(level: LogLevel): Int = level match {
    case Debug => 10
    case Info => 20
    case Warn => 30
    case Error => 40
    case None => 100
  }

  def of(level: String): LogLevel = level.toLowerCase() match {
    case "debug" => Debug
    case "info" => Info
    case "warn" => Warn
    case "error" => Error
    case _ => None
  }
}
