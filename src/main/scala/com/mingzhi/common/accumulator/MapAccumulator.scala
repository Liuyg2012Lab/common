package com.mingzhi.common.accumulator

import org.apache.spark.util.AccumulatorV2

import java.util


/**
 * Map累加器
 * 将二元组的两个值分别映射为Map的kv
 */
class MapAccumulator extends AccumulatorV2[(String, String), java.util.Map[String, String]] {

  private var map = new util.HashMap[String, String]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def reset(): Unit = {
    map.clear()
  }


  override def copy(): AccumulatorV2[(String, String), util.Map[String, String]] = {
    val newAcc = new MapAccumulator()
    newAcc.map = this.map
    newAcc
  }

  override def add(v: (String, String)): Unit = {
    map.put(v._1, v._2)
  }

  override def merge(other: AccumulatorV2[(String, String), util.Map[String, String]]): Unit = {
    other match {
      case o: MapAccumulator => {
        this.map.putAll(o.map)
      }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: util.Map[String, String] = map
}
