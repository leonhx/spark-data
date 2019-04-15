package com.github.leonhx.spark.util

import com.github.leonhx.spark.logging.logger
import org.apache.spark.sql.Dataset

object DuplicationChecker {
  def check[T](counting: Dataset[(T, Long)], tag: String, path: String): Long = {
    import counting.sparkSession.implicits._
    val duplicated = counting.filter(_._2 > 1).cache()
    val duplicateKeysCount = duplicated.count()
    if (duplicateKeysCount > 0) {
      val someDuplicatedKeys = duplicated.map(_._1.toString).take(10).map("      " + _).mkString("\n")
      logger.error(s"[DuplicationChecker:$tag] #duplicated keys: $duplicateKeysCount, path: $path, " +
        s"some keys:\n$someDuplicatedKeys")
      val duplicateRowsCount = duplicated.map(_._2).rdd.sum()
      logger.error(s"[DuplicationChecker:$tag] #duplicated rows: $duplicateRowsCount, path: $path")
    }
    duplicated.unpersist()
    duplicateKeysCount
  }
}
