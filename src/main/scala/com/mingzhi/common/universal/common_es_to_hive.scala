package com.mingzhi.common.universal

import com.mingzhi.ConfigUtils
import com.mingzhi.common.interf.{IDate, LoadStrategy, MySaveMode}
import com.mingzhi.common.universal.common_mysql_to_hive.mysql_host
import com.mingzhi.common.utils.{SinkUtil, SourceUtil, SparkUtils, StringUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 全量抽数支持多表批量导入
 * 支持非默认端口9200
 * 支持非默认_doc
 * 支持历史数据批量抽取
 * 支持三种抽数策略
 * 支持指定存储格式(json,orc,parquet(default))
 * 支持指定分区数
 */
object common_es_to_hive {

  //===========================params define start=====================================
  private var es_host: String = ""
  private var es_indexes = ""

  private var hive_db = ""
  private var hive_tables = ""

  /**
   * "ct_or_ut_time" or ""
   */
  private var create_time = ""
  private var update_time = ""

  private var dt: String = ""
  private var dt1: String = ""
  //===========================params define end=====================================

  private var es_port = "9200"
  private var es_doc = "_doc"
  private var format = "parquet"
  private var partitions = 1


  /**
   * 默认抽数策略为抽新增
   * 实际根据程序传参参数确定策略
   */
  var load_strategy: LoadStrategy.Value = LoadStrategy.NEW

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val builder = SparkUtils.getBuilder

    //本地或者集群
    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")

      es_host = s"${ConfigUtils.IOT_ES_HOST}"
      es_indexes = "wfs_order_material_index_2021"

      hive_db = "paascloud"
      hive_tables = "wfs_order_material_index"

      /**
       * 全量：“”,""
       * 增量之新增：create_time,""
       * 增量之新增和变化：crate_time,update_time
       */
      //      create_time = "orderCreateTime"
      create_time = ""
      //      update_time = "orderUpdateTime"
      update_time = ""

      dt = "2020-12-15"
      dt1 = "2020-12-15"


    } else {
      es_host = args(0)
      es_indexes = args(1) //may be xxx_index:_docx,yyy_index:_docy
      hive_db = args(2)
      hive_tables = args(3)

      create_time = args(4)
      update_time = args(5)

      dt = args(6)
      dt1 = args(7)

      if (args.length == 9) {

        format = processFormat(args(8))

      } else if (args.length == 10) {

        format = processFormat(args(8))

        partitions = if (StringUtils.isEmpty(args(9))) partitions else args(9).toInt
      }
    }

    StringUtil.assertNotBlank(es_host, "es_host can not be null")
    StringUtil.assertNotBlank(es_indexes, "es_indexes can not be null")
    StringUtil.assertNotBlank(hive_db, "hive_db can not be null")
    StringUtil.assertNotBlank(hive_tables, "hive_tables can not be null")


    /**
     * 解析端口
     */
    if (es_host.split(":").length >= 2) {
      es_port = es_host.split(":")(1)
      es_host = es_host.split(":")(0)
    }


    /**
     * 参数解析，确定抽数策略
     */
    if (StringUtils.isBlank(create_time) && StringUtils.isBlank(update_time)) {

      load_strategy = LoadStrategy.ALL
      println("1 load_strategy:" + load_strategy)

    } else if (StringUtils.isNotBlank(create_time) && StringUtils.isBlank(update_time)) {

      load_strategy = LoadStrategy.NEW
      println("2 load_strategy:" + load_strategy)

    } else if (StringUtils.isNotBlank(create_time) && StringUtils.isNotBlank(update_time)) {

      load_strategy = LoadStrategy.NEW_AND_CHANGE
      println("3 load_strategy:" + load_strategy)
    } else {
      throw new IllegalArgumentException("create_time 和 update_time参数组合异常")
    }

    val spark: SparkSession = builder
      .config(ConfigurationOptions.ES_NODES, es_host)
      .config(ConfigurationOptions.ES_PORT, es_port)
      .appName("es_to_hive")
      .getOrCreate()

    println("load_strategy:" + load_strategy)

    if (es_indexes.split(",").length > 1 && load_strategy != LoadStrategy.ALL) {
      throw new IllegalArgumentException("非全量导入不支持多表批量操作...")
    }

    new IDate {
      override def onDate(dt: String): Unit = {

        val indexes = es_indexes.split(",")
        val tables = hive_tables.split(",")

        if (indexes.length != tables.length) {
          throw new IllegalArgumentException("索引数量和hive表数量不一致...")
        }

        indexes.zip(tables).foreach(x => {

          var index = x._1 // may be xxx_index:_doc1
          val table = x._2

          println(s"index is $index and table is $table")

          /**
           * 解析不规范的_doc
           */
          if (index.split(":").length >= 2) {
            es_doc = index.split(":")(1)
            index = index.split(":")(0) // must be xxx_index
          }

          println("es_doc:" + es_doc)

          val frame_result = source(spark, dt, index, es_doc, load_strategy)

          println("frame_result===>")
          frame_result.show(false)

          /**
           * 全量表不进行分区，全表覆盖，只有一份
           */
          val mySaveMode = if (load_strategy == LoadStrategy.ALL) MySaveMode.OverWriteAllTable else MySaveMode.OverWriteByDt

          SinkUtil.sink_to_hive(dt, spark, frame_result, hive_db, table, format, mySaveMode, partitions)
        })

      }
    }.invoke(dt, dt1)

    spark.stop()

  }

  private def processFormat(format: String): String = {

    val result = if (StringUtils.isBlank(format)) "parquet" else format

    result match {
      case "orc" | "parquet" =>
      case _ => throw new IllegalArgumentException("format must be one of orc or parquet,not support json csv text")
    }

    result
  }

  def source(spark: SparkSession, dt: String, t_source: String, _doc: String, load_strategy: LoadStrategy.strategy): DataFrame = {

    var frame_result: DataFrame = null

    if (load_strategy == LoadStrategy.NEW) {

      frame_result = SourceUtil.sourceNewFromEs(spark, index = t_source, _doc, create_time, dt)
        .withColumn("dt", lit(dt))

    } else if (load_strategy == LoadStrategy.ALL) {

      frame_result = SourceUtil.sourceAllFromEs(spark, index = t_source, _doc)
        .withColumn("dt", lit(dt))

    } else if (load_strategy == LoadStrategy.NEW_AND_CHANGE) {

      frame_result = SourceUtil.sourceNewAndChangeFromEs(spark, index = t_source, _doc, create_time, update_time, dt)
        .withColumn("dt", lit(dt))
    }

    frame_result
  }
}
