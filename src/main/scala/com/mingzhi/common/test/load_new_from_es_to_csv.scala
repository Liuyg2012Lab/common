package com.mingzhi.common.test

import com.mingzhi.common.interf.IDate
import com.mingzhi.common.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL


object load_new_from_es_to_csv {

  def main(args: Array[String]): Unit = {

    val conf = SparkUtils.getDefConf

    var areaid = ""
    //    var csvPath = "C:\\Users\\Administrator\\Desktop\\xuchuang"
    var csvPath = "hdfs://mz-hadoop-01:8020/ck/xc"

    val builder = SparkSession.builder()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      conf.host = "192.168.0.163"
      conf.from_db = "wfs_order_list_index"
      conf.from_table = "_doc"
      conf.dt = "2021-01-04"
      conf.dt1 = "2021-01-05"
      conf.createTime = "orderCreateTime"

      areaid = 1329.toString
      builder.master("local[*]")

    } else {
      conf.host = "172.18.253.56"
      conf.from_db = args(0)
      conf.from_table = args(1)
      conf.dt = args(2)
      conf.dt1 = args(3)
      conf.createTime = args(4)
      areaid = args(5)
      csvPath = args(6)
    }
    builder
      .config("es.nodes", conf.host)
      .config("es.port", "9200")
      .config("es.mapping.date.rich", value = false)
      .config("dfs.replication", 1)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config(ConfigurationOptions.ES_SCROLL_SIZE, 10000)
      .config(ConfigurationOptions.ES_MAX_DOCS_PER_PARTITION, 1000000)
      .config(ConfigurationOptions.ES_HTTP_TIMEOUT, "5m")
      .config(ConfigurationOptions.ES_SCROLL_KEEPALIVE, "10m")
      .appName(conf.from_table)
      .enableHiveSupport()
    val spark: SparkSession = builder.getOrCreate()


    new IDate {
      override def onDate(dt: String): Unit = {


        //    val esQuery =
        //      s"""
        //         |
        //         |{
        //         |    "query" : {
        //         |    "bool" : {
        //         |      "must" : [
        //         |        {
        //         |          "range" : {
        //         |            "orderCreateTime" : {
        //         |              "from" : "${conf.dt} 00:00:00",
        //         |              "to" : "${conf.dt1} 23:59:59"
        //         |            }
        //         |          }
        //         |        },
        //         |        {
        //         |          "term" : {
        //         |            "areasId" : {
        //         |              "value" : $areaid
        //         |            }
        //         |          }
        //         |        }
        //         |      ]
        //         |    }
        //         |  }
        //         |}
        //         |
        //         |""".stripMargin

        val esQuery =
          s"""
             |
             |{
             |    "query" : {
             |    "bool" : {
             |      "must" : [
             |        {
             |          "range" : {
             |            "orderCreateTime" : {
             |              "from" : "${conf.dt} 00:00:00",
             |              "to" : "${conf.dt} 23:59:59"
             |            }
             |          }
             |        }
             |      ]
             |    }
             |  }
             |}
             |
             |""".stripMargin

        val resDF: DataFrame = EsSparkSQL.esDF(spark, s"/${conf.from_db}/${conf.from_table}", esQuery)

        resDF.show(false)


        resDF
          .coalesce(1)
          .write
          .mode(SaveMode.Append)
          .option("header", "true")
          .csv(path = csvPath)

      }
    }.invoke(conf.dt, conf.dt1)

    spark.stop()
  }
}
