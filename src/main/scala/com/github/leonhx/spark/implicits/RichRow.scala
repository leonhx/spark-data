package com.github.leonhx.spark.implicits

import org.apache.spark.sql.Row

private[implicits] class RichRow(row: Row) {
  def lookup[T](fieldName: String): Option[T] = {
    val idx = row.fieldIndex(fieldName)
    if (row.isNullAt(idx)) None else Some(row.getAs[T](idx))
  }
}
