package com.github.leonhx.spark

import com.github.leonhx.spark.logging.logger
import com.github.leonhx.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

trait OfflineDataSet[T] extends Serializable {

  /**
    * tag for this data
    */
  protected val tag: String

  /**
    * path to the file
    */
  val path: String

  /**
    * number of partitions when saving
    */
  protected val numPartitions: Int

  private val hooksWhenFinished: mutable.ArrayBuilder[this.type => Unit] = mutable.ArrayBuilder.make()

  private def addFinishHook(action: this.type => Unit): Unit = hooksWhenFinished += action

  private def addFinishHook(action: => Unit): Unit = addFinishHook(_ => action)

  protected def cache[U](dataSet: Dataset[U], storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER): Dataset[U] = {
    addFinishHook(_ => dataSet.unpersist())
    dataSet.persist(storageLevel)
  }

  protected def register[C <: Counter](counter: C): C = {
    SparkUtil.session.sparkContext.register(counter.acc, counter.name)
    addFinishHook(counter.check(tag))
    counter
  }

  protected def register(counter: Counter, counters: Counter*): Unit = {
    register(counter)
    counters.foreach(register)
  }

  /**
    * fetch data set from the source
    *
    * @return the data set
    */
  protected def fetch(): Dataset[T]

  /**
    * fetch data set from the source and then dump to the `path`
    */
  final def update(): Unit = {
    val tempPath = SparkUtil.generatePathLike(s"${tempPathOf(path)}/version=%d")
    val sparkSession = SparkUtil.session

    cache(fetch()).write.parquet(tempPath)

    sparkSession.sqlContext.clearCache()
    hooksWhenFinished.result().foreach(_.apply(this))

    logger.debug(s"[OfflineDataSet:$tag] updated temp path: $tempPath. Checking now ...")
    val isCheckingPass = check(convert(sparkSession.read.parquet(tempPath)))
    sparkSession.sqlContext.clearCache()

    if (!isCheckingPass) {
      val debugPath = SparkUtil.generatePathLike(s"${debugPathOf(path)}/version=%d")
      logger.error(s"[OfflineDataSet:$tag] invalid data for $path, move to $debugPath")

      sparkSession.read.parquet(tempPath)
        .write.parquet(debugPath)
      SparkUtil.deleteIfExists(tempPath)

      throw new IllegalArgumentException(s"invalid data set: $debugPath")
    } else {
      logger.debug(s"[OfflineDataSet:$tag] checking pass, move to $path")

      sparkSession.read.parquet(tempPath)
        .repartition(numPartitions)
        .write.parquet(path)
      SparkUtil.deleteIfExists(tempPath)

      logger.debug(s"[OfflineDataSet:$tag] successfully updated: $path")
    }
  }

  /**
    * whether data exists in the `path`
    *
    * @return true if exists, otherwise false
    */
  final def exists(): Boolean =
    SparkUtil.exists(path) && SparkUtil.nonEmptyDir(path) && SparkUtil.exists(SparkUtil.join(path, "_SUCCESS"))

  /**
    * @param dataSet of type T
    * @return whether the data set is valid
    */
  protected def check(dataSet: Dataset[T]): Boolean

  /**
    * whether dumped data set is valid
    *
    * @return true if valid, otherwise false
    */
  final def isValid: Boolean = exists() && check(load())

  /**
    * convert a data frame to data set
    *
    * @param dataFrame loaded from hdfs
    * @return the data set
    */
  protected def convert(dataFrame: DataFrame): Dataset[T]

  /**
    * load data set from the `path`
    *
    * @return the data set
    */
  final def load(): Dataset[T] = convert(SparkUtil.session.read.parquet(path))

  // TODO:
  private def tempPathOf(relativePath: String): String = s"<path for temp data>/temp/$relativePath"

  // TODO:
  private def debugPathOf(relativePath: String): String = s"<path for temp data>/debug/$relativePath"
}
