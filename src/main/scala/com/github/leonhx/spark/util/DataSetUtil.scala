package com.github.leonhx.spark.util

object DataSetUtil {
  def loadFeatureMap(path: String): Array[(Int, String)] = {
    val meta = SparkUtil.sparkContext.textFile(path).map { row =>
      val Array(fId, fName, "q") = row.split("\t")
      (fId.toInt, fName)
    }.collect()

    val featureIds = meta.map(_._1)
    if (featureIds.min != 1) {
      throw new IllegalArgumentException("expect min feature id to be 1")
    }
    if (featureIds.max != meta.length) {
      throw new IllegalArgumentException(s"expect max feature id to be ${meta.length}")
    }
    if (featureIds.toSet.size != meta.length) {
      throw new IllegalArgumentException(s"require no duplicate feature ids")
    }

    meta.sortBy(_._1)
  }

  def saveFeatureMap(path: String, featureNames: Seq[String]): Unit =
    SparkUtil.sparkContext
      .makeRDD(featureNames.zipWithIndex.map { case (fName, i) => s"${i + 1}\t$fName\tq" }, numSlices = 1)
      .saveAsTextFile(path)
}
