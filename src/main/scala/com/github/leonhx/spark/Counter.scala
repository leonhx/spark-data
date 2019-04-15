package com.github.leonhx.spark

import com.github.leonhx.spark.logging.logger
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.concurrent.TrieMap

trait Counter {
  val name: String

  val acc: AccumulatorV2[_, _]

  def check(tag: String): Unit
}

class SimpleCounter(override val name: String) extends Counter with Serializable {
  override val acc: LongAccumulator = new LongAccumulator()

  def incr(n: Long = 1L): Unit = acc.add(n)

  override def check(tag: String): Unit = if (acc.value > 0)
    logger.error(s"[SimpleCounter:$tag] $name: ${acc.value}")
}

class Collector[T](override val name: String, maxSize: Int)(implicit ord: Ordering[T])
  extends Counter with Serializable {
  override val acc: AccumulatorOfCollector[T] =
    new AccumulatorOfCollector[T](TrieMap.empty[T, Int], maxSize)

  def add(example: T): Unit = acc.add(example)

  override def check(tag: String = ""): Unit =
    acc.value.toSeq.sortBy(_._1).foreach { case (example, count) =>
      if (count > 0) {
        logger.error(s"[Collector${if (tag.isEmpty) "" else s":$tag"}] [$name] $example: $count")
      } else throw new RuntimeException("should not arrive here")
    }
}

class KeyCounter[T](override val name: String, keyName: String)(implicit ord: Ordering[T])
  extends Counter with Serializable {
  override val acc: AccumulatorOfKeyCounter[T] =
    new AccumulatorOfKeyCounter[T](TrieMap.empty[T, Long])

  def incrBy(key: T, n: Long = 1L): Unit = acc.add(key -> n)

  override def check(tag: String = ""): Unit =
    acc.value.toSeq.sortBy(_._1).foreach { case (key, count) =>
      if (count > 0) {
        logger.error(s"[KeyCounter${if (tag.isEmpty) "" else s":$tag"}] [$keyName=$key] $name: $count")
      } else throw new RuntimeException("should not arrive here")
    }
}

private[spark] class AccumulatorOfKeyCounter[T](counters: TrieMap[T, Long])
  extends AccumulatorV2[(T, Long), TrieMap[T, Long]] {
  override def isZero: Boolean = counters.isEmpty

  override def copy(): AccumulatorV2[(T, Long), TrieMap[T, Long]] =
    new AccumulatorOfKeyCounter[T](counters.collect { case x => x })

  override def reset(): Unit = counters.clear()

  override def add(kv: (T, Long)): Unit = kv match {
    case (k, v) => counters(k) = counters.getOrElse(k, 0L) + v
  }

  override def merge(other: AccumulatorV2[(T, Long), TrieMap[T, Long]]): Unit = other.value foreach {
    case (k, v) => counters(k) = counters.getOrElse(k, 0L) + v
  }

  override def value: TrieMap[T, Long] = counters
}

private[spark] class AccumulatorOfCollector[T](examples: TrieMap[T, Int], maxSize: Int)
  extends AccumulatorV2[T, TrieMap[T, Int]] {
  override def isZero: Boolean = examples.isEmpty

  override def copy(): AccumulatorV2[T, TrieMap[T, Int]] =
    new AccumulatorOfCollector[T](examples.collect { case x => x }, maxSize)

  override def reset(): Unit = examples.clear()

  override def add(e: T): Unit =
    if (examples contains e) examples(e) += 1
    else if (examples.size < maxSize) examples(e) = 1

  override def merge(other: AccumulatorV2[T, TrieMap[T, Int]]): Unit = other.value foreach {
    case (e, c) =>
      if (examples contains e) examples(e) += c
      else if (examples.size < maxSize) examples(e) = c
  }

  override def value: TrieMap[T, Int] = examples
}
