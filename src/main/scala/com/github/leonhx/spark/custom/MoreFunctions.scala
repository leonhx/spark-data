package com.github.leonhx.spark.custom

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object MoreFunctions {
  private val _is_missing = udf { v: Double => v != v }
  private val _freq = udf { v: Double => if (v != v) 0L else 1L }
  private val _value = udf { v: Double => if (v != v) 0D else v }

  /**
    * @param column of type [[Double]]
    * @return a column of type [[Double]]
    */
  def avg_val(column: Column): Column = sum_val(column) / count_val(column)

  /**
    * @param column of type [[Double]]
    * @return a column of type [[Double]]
    */
  def sum_val(column: Column): Column = sum(_value(column))

  /**
    * @param column of type [[Double]]
    * @return a column of type [[Double]]
    */
  def count_val(column: Column): Column = sum(_freq(column))

  /**
    * @param column of type [[Double]]
    * @return a column of type [[Double]]
    */
  def std_val(column: Column): Column = sqrt(avg_val(column * column) - avg_val(column) * avg_val(column))

  /**
    * @param column of type [[Double]]
    * @return a column of type [[Boolean]]
    */
  def is_missing(column: Column): Column = _is_missing(column)
}
