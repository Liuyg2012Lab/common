package com.mingzhi.common.universal

import com.mingzhi.common.interf.{IDate, MySaveMode}
import com.mingzhi.common.utils.{SinkUtil, SourceUtil, SparkUtils}
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.slf4j.LoggerFactory

/**
 * 导新增和变化 wfs_order_list_index
 */
@deprecated
object load_new_change_from_es {

  private var es_port = "9200"

  private val logger = LoggerFactory.getLogger(load_new_change_from_es.getClass)

  def main(args: Array[String]): Unit = {

    val conf: SparkUtils.DefConf = SparkUtils.getDefConf

    val builder = SparkUtils.getBuilder


    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      conf.host = "192.168.0.163"
      conf.from_db = "wfs_order_list_index"
      conf.from_table = "_doc"
      conf.to_db = "paascloud"
      conf.to_table = conf.from_db
      conf.dt = "2020-12-16"
      conf.dt1 = "2020-12-16"

      conf.createTime = "orderCreateTime"
      conf.updateTime = "orderUpdateTime"

      builder.master("local[*]")

    } else {

      if (args.length != 10) {

        throw new IllegalArgumentException("需要10个参数,分别是es地址，es库，es表," +
          "目标库，目标表，起始时间，结束时间，" +
          "数据的创建时间字段，数据的更新时间字段，数据存储格式(parquet or json)")

      }

      conf.host = args(0)
      conf.from_db = args(1)
      conf.from_table = args(2)
      conf.to_db = args(3)
      conf.to_table = args(4)
      conf.dt = args(5)
      conf.dt1 = args(6)
      conf.createTime = args(7)
      conf.updateTime = args(8)
      conf.format = args(9)
    }

    if (conf.host.split(":").length >= 2) {
      es_port = conf.host.split(":")(1)
      conf.host = conf.host.split(":")(0)
    }

    try {
      val spark: SparkSession = builder
        .config(ConfigurationOptions.ES_NODES, conf.host)
        .config(ConfigurationOptions.ES_PORT, es_port)
        .appName(conf.from_db + ":" + conf.from_table)
        .getOrCreate()


      new IDate {
        override def onDate(dt: String): Unit = {

          println("回调日期:" + dt)
          process(spark, conf.from_db, conf.from_table, conf.to_db, conf.to_table, dt, conf.createTime, conf.updateTime, conf.format)

        }
      }.invoke(conf.dt, conf.dt1)

      spark.stop()
      logger.info("The task execution completed ......")
    } catch {
      case e: SparkException => {
        logger.error("The spark job error info : {}", e)
        logger.error("The task execution exception ......")
        System.exit(-1)
      }
    }

  }

  private def process(spark: SparkSession, from_db: String, from_table: String, to_db: String, to_table: String, dt: String, ct: String, ut: String, format: String): Unit = {

    /**
     * 每一条数据打上计算日期dt的标签，==>ods的对应分区
     */
    val frame_result = SourceUtil.sourceNewAndChangeFromEs(spark, index = from_db, from_table, ct, ut, dt)
      .withColumn("dt", lit(dt))

    SinkUtil.sink_to_hive(dt, spark, frame_result, to_db, to_table, format, MySaveMode.OverWriteByDt, 1)

  }
}
