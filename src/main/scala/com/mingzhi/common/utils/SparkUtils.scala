package com.mingzhi.common.utils

import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object SparkUtils {

  def getDefConf = {
    System.setProperty("HADOOP_USER_NAME", "root")

    var dbServer = "192.168.0.201"
    var from_db = ""
    var from_table = ""
    var to_db = ""
    var to_table = ""
    var user_name = ""
    var password = ""
    var dt = "2020-06-01"
    var dt1 = "2020-06-02"
    var format = "parquet"

    DefConf(dbServer, from_db, from_table, to_db, to_table, user_name, password, dt, dt1, createTime = "createTime", updateTime = "updateTime", format = "parquet")
  }

  case class DefConf(var host: String, var from_db: String, var from_table: String, var to_db: String, var to_table: String, var user_name: String, var password: String, var dt: String, var dt1: String, var createTime: String, var updateTime: String, var format: String)


  def getBuilder: SparkSession.Builder = {

    val builder = SparkSession.builder()
      .config("dfs.replication", 1)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .config(ConfigurationOptions.ES_PORT, 9200)
      .config(ConfigurationOptions.ES_MAPPING_DATE_RICH_OBJECT, false)
      .config(ConfigurationOptions.ES_SCROLL_SIZE, 10000)
      .config(ConfigurationOptions.ES_MAX_DOCS_PER_PARTITION, 1000000)
      .config(ConfigurationOptions.ES_HTTP_TIMEOUT, "5m")
      .config(ConfigurationOptions.ES_SCROLL_KEEPALIVE, "10m")
      .enableHiveSupport()

    builder

  }
}
