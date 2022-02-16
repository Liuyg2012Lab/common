package com.mingzhi.common.universal

import com.mingzhi.common.interf
import com.mingzhi.common.interf.MySaveMode.MySaveMode
import com.mingzhi.common.interf.{IDate, MySaveMode}
import com.mingzhi.common.utils.SinkUtil.logger
import com.mingzhi.common.utils.{SinkUtil, SparkUtils, StringUtil}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.Date

/**
 * 通用的从数仓同步数据到mysql的类
 * 导出数据时必须先在mysql建立好表结构
 * 支持按dt覆盖或者全表覆盖
 */
object hive_to_mysql {
  private val logger = LoggerFactory.getLogger(hive_to_mysql.getClass)

  def main(args: Array[String]): Unit = {


    //    var mysql_host = ConfigUtils.ABI_MYSQL_HOST
    //    var from_db = "paascloud"
    //    var from_table = "ads_order_address_cnts"
    //    var to_db = "mz_olap"
    //    var to_table = "wfs_ads_order_address_cnts"
    //    var userName = ConfigUtils.ABI_MYSQL_USERNAME
    //    var passWd = ConfigUtils.ABI_MYSQL_PWD

    //    var mysql_host = "192.168.0.169"
    //    var from_db = "paascloud"
    //    var from_table = "ads_order_csgj"
    //    var to_db = "paascloud_wfs"
    //    var to_table = "ads_order_csgj"
    //    var userName = "mzadmin"
    //    var passWd = "Mz@123456"

    //    var mysql_host = s"${ConfigUtils.SEGI_MYSQL_HOST}:3308"
    //    var from_db = "contract"
    //    var from_table = "dwd_contract"
    //    var to_db = "contract"
    //    var to_table = "segi_dwd_contract"
    //    var userName = ConfigUtils.SEGI_MYSQL_USERNAME
    //    var passWd = ConfigUtils.SEGI_MYSQL_PWD

    var mysql_host = "192.168.20.179:3308"
    var from_db = "hr"
    var from_tables = "dwd_staff_info"
    var to_db = "vwork"
    var to_tables = "dwd_staff_info"
    var userName = "root"
    var passWd = "123456"

    var dt = "2021-10-13"
    var dt1 = "2021-10-13"

    var saveMode = MySaveMode.OverWriteByDt

    System.setProperty("HADOOP_USER_NAME", "root")
    val builder = SparkUtils.getBuilder


    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")
    } else {
      try {
        mysql_host = args(0)
        from_db = args(1)
        from_tables = args(2)
        to_db = args(3)
        to_tables = args(4)
        userName = args(5)
        passWd = args(6)
        dt = args(7)
        dt1 = args(8)

        if (args.length == 10) {

          saveMode = defineSaveMode(args(9))

        } else if (args.length == 11) {

          saveMode = defineSaveMode(args(9))
          //next params

        }

        StringUtil.assertNotBlank(mysql_host, "mysql_host can not be null")
        StringUtil.assertNotBlank(from_db, "from_db can not be null")
        StringUtil.assertNotBlank(from_tables, "from_tables can not be null")
        StringUtil.assertNotBlank(to_db, "to_db can not be null")
        StringUtil.assertNotBlank(to_tables, "to_tables can not be null")
        StringUtil.assertNotBlank(userName, "userName can not be null")
        StringUtil.assertNotBlank(passWd, "passWd can not be null")

        if (from_tables.split(",").length != to_tables.split(",").length) {
          throw new IllegalArgumentException("源表和目标表数量不一致...")
        }


      } catch {
        case e: Exception => logger.error("Parameter exception: {} ", e)
          logger.error("Parameter order is {}", "【mysql_host\nfrom_db\nfrom_table\nto_db\nto_table\nuserName\npassWd\ndt\ndt1】")
          System.exit(-1)
      }
    }


    try {
      val spark = builder
        .appName("hive_to_mysql")
        .getOrCreate()

      from_tables.split(",").zip(to_tables.split(",")).foreach(r => {

        process(spark, mysql_host, from_db, r._1, to_db, r._2, userName, passWd, dt, dt1, saveMode)

      })

      spark.stop()
      logger.info("The task execution completed ......")
    } catch {
      case e: SparkException => logger.error("SparkException:DatabasesName={},TableName={},exception info {}", from_db, to_tables, e.getMessage)
      case e: Exception => logger.error("Exception:DatabasesName={},TableName={},exception info {}", from_db, to_tables, e.getMessage)
      case _: java.lang.Exception => logger.error("The spark task failed to write data to mysql database,未知异常")
        System.exit(-1)
    }
  }

  def process(spark: SparkSession, mysql_host: String, from_db: String, from_table: String
              , to_db: String, to_table: String, userName: String, passWd: String, dt: String, dt1: String, saveMode: MySaveMode): Unit = {

    new IDate {
      override def onDate(dt: String): Unit = {

        val frame = spark.sql(
          s"""
             |select *,'${DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")}' as last_update_time from $from_db.$from_table where dt='$dt'
             |""".stripMargin)

        frame.show(true)

        SinkUtil.sink_to_mysql(mysql_host, to_db, to_table, userName, passWd, frame, 5, saveMode, dt)

      }
    }.invoke(dt, dt1)

  }

  def defineSaveMode(saveMode: String): interf.MySaveMode.Value = {
    var mode = MySaveMode.OverWriteByDt

    if (MySaveMode.OverWriteByDt.toString.equalsIgnoreCase(saveMode)) {
      mode = MySaveMode.OverWriteByDt
    } else if (MySaveMode.OverWriteAllTable.toString.equalsIgnoreCase(saveMode)) {
      mode = MySaveMode.OverWriteAllTable
    }

    mode
  }
}
