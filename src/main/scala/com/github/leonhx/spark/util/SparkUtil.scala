package com.github.leonhx.spark.util

import com.dianping.midas.ranker.common4s.util.CollectionUtil
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SQLContext, SQLImplicits}

object SparkUtil {
  def session: SparkSession = session(identity[SparkSession.Builder])

  def session(op: SparkSession.Builder => SparkSession.Builder): SparkSession =
    op(SparkSession.builder().enableHiveSupport()).getOrCreate()

  def sparkContext: SparkContext = session.sparkContext

  def sqlContext: SQLContext = session.sqlContext

  object implicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = sqlContext
  }

  def withAppRenamed(rename: String => String): SparkSession =
    SparkSession.builder()
      .appName(rename(session.conf.get("spark.app.name")))
      .getOrCreate()

  def fs: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

  def listFiles(dirPath: String): Array[FileStatus] = fs.listStatus(new Path(dirPath))

  def listFileNames(dirPath: String): Array[String] = listFiles(dirPath).map(_.getPath.getName)

  def join(path: String, fileName: String, fileNames: String*): String =
    (Seq(path.stripSuffix(Path.SEPARATOR), fileName) ++ fileNames).mkString(Path.SEPARATOR)

  def split(path: String): (String, String) = {
    val pathParts = path.split(Path.SEPARATOR_CHAR)
    (pathParts.init.mkString(Path.SEPARATOR), pathParts.last)
  }

  def exists(path: String): Boolean = fs.exists(new Path(path))

  def isEmptyDir(dirPath: String): Boolean = listFiles(dirPath).isEmpty

  def nonEmptyDir(dirPath: String): Boolean = !isEmptyDir(dirPath)

  def delete(path: String): Boolean =
    try fs.delete(new Path(path), true) catch {
      case _: Throwable => false
    }

  def deleteIfExists(path: String): Boolean = if (exists(path)) delete(path) else false

  def generatePathLike(filePattern: String): String =
    CollectionUtil.stream(1).map(filePattern.format(_)).find(!exists(_)).get
}
