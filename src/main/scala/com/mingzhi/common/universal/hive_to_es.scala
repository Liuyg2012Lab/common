package com.mingzhi.common.universal

import java.util.Date

import com.mingzhi.common.interf.{IDate, MySaveMode}
import com.mingzhi.common.utils.{SinkUtil, TableUtils}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory

/**
 * 通用的从数仓同步数据到ElasticSearch的类
 */
object hive_to_es {
  private val logger = LoggerFactory.getLogger(hive_to_change_mysql.getClass)
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    var from_db = "paascloud_tcm"
    var from_table = "ads_t_task_template_common"

    var es_host = "192.168.20.145"
    var to_index = "tcm_task_code_index"

    var dt = "2020-11-01"
    var dt1 = "2020-11-02"


    val builder = SparkSession.builder()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      builder.master("local[*]")
    } else {
      try {
        from_db = args(0)
        from_table = args(1)
        es_host = args(2)
        to_index = args(3)
        dt = args(4)
        dt1 = args(5)
      } catch {
        case e:Exception =>logger.error("Parameter exception: {} ",e)
          logger.error("Parameter order is {}", "【mysql_host\nfrom_db\nfrom_table\nto_db\nto_table\nuserName\npassWd\ndt\ndt1】")
          System.exit(-1)
      }
    }


    try {
      builder.config("spark.sql.parquet.writeLegacyFormat", "true")
        .enableHiveSupport()
        .appName(from_table + "_to_change_mysql")
      val spark = builder.getOrCreate()

      new IDate {
        override def onDate(dt: String): Unit = {
          val frame = spark.sql(
            s"""
               |select *,'${DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")}' as last_update_time from $from_db.$from_table where dt='$dt'
               |""".stripMargin)
          frame.show()

          val map = new scala.collection.mutable.HashMap[String, String]
          map += ConfigurationOptions.ES_NODES -> es_host

          EsSparkSQL.saveToEs(frame.repartition(3), s"/${to_index}/_doc", map)
        }
      }.invoke(dt, dt1)

      spark.stop()
      logger.info("The task execution completed ......")
    } catch {
      case  e:SparkException=> {
        logger.error("DatabasesName={},TableName={},exception info {}",es_host,to_index,e.getMessage)
        logger.error("The task execution exception ......")
        System.exit(-1)
      }
    }



  }
}
