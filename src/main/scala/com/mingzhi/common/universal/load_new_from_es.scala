package com.mingzhi.common.universal

import com.mingzhi.common.interf.IDate
import com.mingzhi.common.utils.{SinkUtil, SourceUtil, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 导新增
 * 如工单轨迹表 wfs_order_track_index
 */
@deprecated
object load_new_from_es {

  private var es_port = "9200"

  def main(args: Array[String]): Unit = {

    val conf = SparkUtils.getDefConf

    val builder = SparkUtils.getBuilder

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      conf.host = "192.168.0.207"
      conf.from_db = "wfs_order_track_index"
      conf.from_table = "_doc"
      conf.to_db = "paascloud"
      conf.to_table = conf.from_db
      conf.dt = "2020-06-16"
      conf.dt1 = "2020-06-20"
      conf.createTime = "createTime"

      builder.master("local[*]")

    } else {
      conf.host = args(0)
      conf.from_db = args(1)
      conf.from_table = args(2)
      conf.to_db = args(3)
      conf.to_table = args(4)
      conf.dt = args(5)
      conf.dt1 = args(6)
      conf.createTime = args(7)
    }

    if (conf.host.split(":").length >= 2) {
      es_port = conf.host.split(":")(1)
      conf.host = conf.host.split(":")(0)
    }

    val spark: SparkSession = builder
      .config(ConfigurationOptions.ES_NODES, conf.host)
      .config(ConfigurationOptions.ES_PORT, es_port)
      .appName(conf.from_db + ":" + conf.from_table)
      .getOrCreate()


    new IDate {
      override def onDate(dt: String): Unit = {

        val frame_result = SourceUtil.sourceNewFromEs(spark, index = conf.from_db, conf.from_table, conf.createTime, dt)
          .withColumn("dt", lit(dt))

        SinkUtil.sink_to_hive(dt, spark, frame_result, conf.to_db, conf.to_table)

      }
    }.invoke(conf.dt, conf.dt1)
    spark.stop()
  }
}
