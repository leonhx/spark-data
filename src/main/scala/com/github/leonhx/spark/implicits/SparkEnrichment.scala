package com.github.leonhx.spark.implicits

import org.apache.spark.sql.{Dataset, Row}

import scala.language.implicitConversions

object SparkEnrichment {
  implicit def toRichDataSet[T](dataSet: Dataset[T]): RichDataSet[T] = new RichDataSet(dataSet)

  implicit def toRichRow(row: Row): RichRow = new RichRow(row)
}
