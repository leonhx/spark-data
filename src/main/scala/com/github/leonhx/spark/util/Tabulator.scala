package com.github.leonhx.spark.util

object Tabulator {
  def format(table: Seq[Seq[Any]], headingSpace: Int = 0): String =
    format(table.head, table.tail, headingSpace)

  def format(header: Seq[Any], table: Seq[Seq[Any]], headingSpace: Int): String =
    table match {
      case Seq() => ""
      case _ =>
        val sizes = for (row <- header +: table)
          yield for (cell <- row)
            yield if (cell == null) 0 else cell.toString.length
        val colSizes = for (col <- sizes.transpose) yield col.max
        val rows = for (row <- table) yield formatRow(row, colSizes)
        formatRows(rowSeparator(colSizes), formatRow(header, colSizes), rows, headingSpace)
    }

  private def formatRows(rowSep: String, header: String, rows: Seq[String], space: Int): String = {
    val heading = List(rowSep, header, rowSep)
    val formattedRows = if (space <= 0) heading ++ rows else
      rows.grouped(space).flatMap(heading ++ _).toSeq
    (formattedRows :+ rowSep).mkString("\n")
  }

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = for ((item, size) <- row.zip(colSizes))
      yield if (size == 0) "" else ("%" + size + "s").format(item)
    cells.mkString("|", "|", "|")
  }

  private def rowSeparator(colSizes: Seq[Int]): String = colSizes.map("=" * _)
    .mkString("+", "+", "+")
}
