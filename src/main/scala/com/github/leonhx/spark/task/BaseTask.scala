/* Copyright (C) 2017-2017 Meituan.com, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
package com.github.leonhx.spark.task

import com.github.leonhx.spark.logging.logger
import com.github.leonhx.spark.util.{Config, Parse, SparkUtil}
import org.joda.time.DateTime

trait BaseTask {
  def withConfig[T](args: Array[String], defaultConf: Config = Config())
                   (func: Config => T): T = {
    val conf = defaultConf ++ Parse.args(args)

    SparkUtil.session(_.appName(conf.appName))

    val logLevel = conf("log.level")
    logger.setLogLevel(conf("log.level"))
    logger.setSaveLevel(conf.get("log.save.level").getOrElse(logLevel))
    logger.setLogRoot(conf("log.root"))

    val now = DateTime.now()
    val result = func(conf)
    logger.save(conf.appName.replaceAll("[^a-zA-Z0-9.-]", "_"), now)
    result
  }
}
