package com.github.leonhx.spark.util

import com.dianping.midas.ranker.common4s.util.DateTimeUtil
import com.github.leonhx.spark.util.Config.Key
import org.joda.time.DateTime

import scala.collection.immutable.ListMap

object Config {

  object Key {
    val AppName = "app.name"
    val DaysDelta = "days.delta"
    val DateFrom = "date.from"
    val DateTo = "date.to"
  }

  def apply(kvPairs: Map[String, String]): Config = apply(kvPairs.toSeq: _*)

  def apply(kvPairs: (String, String)*): Config = new Config(ListMap(kvPairs: _*))

  def empty: Config = apply()
}

class Config(private[Config] val kvPairs: ListMap[String, String]) extends Serializable {
  override def toString: String = kvPairs.map { case (k, v) =>
    s"  $k -> $v"
  }.mkString("{\n", ",\n", "\n}")

  def update(other: Config) = Config(kvPairs ++ other.kvPairs)

  def ++(other: Config): Config = update(other)

  def apply(key: String): String = kvPairs(key)

  def get(key: String): Option[String] = kvPairs.get(key)

  def isDefined(key: String): Boolean = kvPairs.contains(key)

  def getInt(key: String): Option[Int] = get(key).map(_.toInt)

  def getLong(key: String): Option[Long] = get(key).map(_.toLong)

  def getDouble(key: String): Option[Double] = get(key).map(_.toDouble)

  def getBoolean(key: String): Option[Boolean] = get(key).map(_.toBoolean)

  def getArray(key: String): Option[Array[String]] = get(key).map(_.split(","))

  def contains(key: String): Boolean = kvPairs.contains(key)

  def keySet: Set[String] = kvPairs.keySet

  /**
    * 返回当前配置的日期
    *
    * 1. 检查 days.delta , 如果存在, 则取 days.delta 天之前的日期
    * 2. 检查 date , 格式 yyyy-MM-dd, 如果存在, 则取这个日期
    * 3. 检查 dt , 格式 yyyyMMdd, 如果存在, 则取这个日期
    * 4. 检查 date.from 与 date.to , 格式 yyyy-MM-dd, 取从 date.from 到 date.to 之间的所有日期(起始日期均包含)
    *
    * @return 所有被选中的日期
    */
  def dates(now: DateTime): Seq[DateTime] = getInt(Key.DaysDelta) match {
    case Some(d) => Seq(now.minusDays(d))
    case None => get("date") match {
      case Some(dateRepr) => Seq(DateTimeUtil.parseDate(dateRepr))
      case None =>
        get("dt") match {
          case Some(dtRepr) => Seq(DateTimeUtil.parseDateKey(dtRepr))
          case None =>
            val startDate = DateTimeUtil.parseDate(apply(Key.DateFrom))
            val endDate = DateTimeUtil.parseDate(apply(Key.DateTo))

            DateTimeUtil.datesBetween(startDate, endDate)
        }
    }
  }

  lazy val appName: String = apply(Key.AppName)
}