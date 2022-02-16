package com.mingzhi.common.utils

import org.apache.spark.sql.SparkSession

object HiveUtil {


  /**
   * 开启动态分区，非严格模式
   *
   * @param spark
   */
  def openDynamicPartition(spark: SparkSession) = {
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set spark.sql.shuffle.partitions=3")
  }


  /**
   * 使用snappy压缩
   *
   * @param spark
   */
  def useSnappyCompression(spark: SparkSession) = {
    spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
    spark.sql("set mapreduce.output.fileoutputformat.compress=true")
    spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }

}
