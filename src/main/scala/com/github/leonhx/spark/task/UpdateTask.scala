package com.github.leonhx.spark.task

import com.github.leonhx.spark.OfflineDataSet
import com.github.leonhx.spark.logging.logger
import com.github.leonhx.spark.util.SparkUtil

object UpdateTask {
  def execute(data: OfflineDataSet[_]): Unit = {
    logger.debug(s"[UpdateTask.execute] path to update: ${data.path}")
    if (data.exists()) {
      logger.info(s"[UpdateTask.execute] path exists: ${data.path}")
    } else {
      SparkUtil.deleteIfExists(data.path)
      data.update()
      logger.debug(s"[UpdateTask.execute] path is updated: ${data.path}")
    }
  }

  def execute(allData: Seq[OfflineDataSet[_]]): Unit = allData.foreach(execute)
}
