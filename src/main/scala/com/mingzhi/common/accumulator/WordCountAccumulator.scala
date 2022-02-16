package com.mingzhi.common.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

  var map: mutable.Map[String, Long] = mutable.Map()

  override def isZero: Boolean = {
    println("process isZero")
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {

    println("process copy")
    val acc = new WordCountAccumulator()
    acc.map = this.map
    acc

  }

  override def reset(): Unit = {
    println("process reset")
    map.clear()
  }

  override def add(word: String): Unit = {
    println("process add")
    // 查询map中是否存在相同的单词
    // 如果有相同的单词，那么单词的数量加1
    // 如果没有相同的单词，那么在map中增加这个单词
    map(word) = map.getOrElse(word, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    println("process merge")
    val map1 = map
    val map2 = other.value

    // 两个Map的合并
    map = map1.foldLeft(map2)(
      (innerMap, kv) => {
        innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2
        innerMap
      }
    )
  }

  override def value: mutable.Map[String, Long] = {
    println("process value")
    map
  }
}
