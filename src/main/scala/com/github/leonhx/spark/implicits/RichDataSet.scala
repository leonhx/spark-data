package com.github.leonhx.spark.implicits

import org.apache.spark.sql.{Column, DataFrame, Dataset}

private[implicits] class RichDataSet[T](left: Dataset[T]) {
  def leftOuterJoin[U](right: Dataset[U], condition: Column): Dataset[(T, U)] = left.joinWith(right, condition, "left_outer")

  def toDF(colNames: String*): DataFrame = left.toDF().toDF(colNames: _*)
}
