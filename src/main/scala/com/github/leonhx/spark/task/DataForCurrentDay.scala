package com.github.leonhx.spark.task

import com.dianping.midas.ranker.common4s.util.DateTimeUtil
import com.github.leonhx.spark.util.SparkUtil
import org.joda.time.DateTime

trait DataForCurrentDay {
  def checkDate(date: DateTime): Unit = {
    require(date.isBeforeNow, s"invalid date $date")
    SparkUtil.withAppRenamed(n => s"$n(${DateTimeUtil.formatDateKey(date)})")
  }
}
