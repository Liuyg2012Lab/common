package com.mingzhi.common.universal

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Date

import com.mingzhi.common.interf.{IDate, MySaveMode}
import com.mingzhi.common.utils.{DbUtils, SinkUtil, TableUtils}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * 通用的从数仓同步数据到mysql的类
 * 导出数据时必须先在mysql建立好表结构
 * 如果数据有修改，需要同时导入多天数据
 */
object hive_to_change_mysql {
  private val logger = LoggerFactory.getLogger(hive_to_change_mysql.getClass)
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    var dt = "2020-08-12"
    var dt1 = "2020-08-12"

    var dbServer = "192.168.0.201"
    var from_db = "paascloud"
    //    var from_table = "ads_order_overall_cube"
    //    var from_table = "ads_order_result_cube"
    var from_table = "ads_order_exception_cube"
    var to_db = "paascloud_wfs"
    var to_table = from_table
    var userName = "root"
    var passWd = "Mmz@2020"
    var updateTableNames = "db.tableA,db.tableB,db.tableC,db.tableD";   //用那些表判断数据是否修改 格式如下 db.tableA,db.tableB,db.tableC,db.tableD
    var isUpdate:Boolean = false

    val builder = SparkSession.builder()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      builder.master("local[*]")
    } else {
      try {
        dbServer = args(0)
        from_db = args(1)
        from_table = args(2)
        to_db = args(3)
        to_table = args(4)
        userName = args(5)
        passWd = args(6)
        dt = args(7)
        dt1 = args(8)
        if (args.length >= 10) { //updateTableNames is not null
          updateTableNames = args(7)
          dt = args(8)
          dt1 = args(9)
          isUpdate = true
        }
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
      val driver = "com.mysql.jdbc.Driver"
      val url = s"jdbc:mysql://$dbServer:3306/$to_db?connectionRequestTimout=60000&connectionTimeout=60000&socketTimeout=60000"

      new IDate {
        override def onDate(dt: String): Unit = {
          var dates = Array(dt)
          if(isUpdate){
            try {
              dates = TableUtils.queryExecuteDate(spark, updateTableNames, dt)//获取新增和修改的日期
            } catch {
              case  e:Exception => logger.error("get update dates is Failed {}",e)
                System.exit(-1)
            }
          }

          dates.foreach(date=>{
            val frame = spark.sql(
              s"""
                 |select *,'${DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")}' as last_update_time from $from_db.$from_table where dt='$date'
                 |""".stripMargin)
            frame.show()
            SinkUtil.sink_to_mysql(dbServer, to_db, to_table, userName, passWd, frame, 3, MySaveMode.OverWriteByDt, date)
/*            val frame_result = frame.coalesce(3)
            frame_result.foreachPartition(it => {
              Class.forName(driver)
              var conn: Connection = null
              var pstmt: PreparedStatement = null

              try {
                conn = DriverManager.getConnection(url, userName, passWd)
                pstmt = conn.prepareStatement(s"DELETE FROM $to_table WHERE dt='$date'")

                println(s"删除${date}数据...")
                pstmt.execute()
              } catch {
                case e: Exception => logger.error("MySQL connection exception {}:",e)
                  System.exit(-1)
              } finally {
                DbUtils.release(conn, pstmt)
              }
            })

            println(s"导入${date}新的数据")

            try {
              frame_result.write
                .format("jdbc")
                .option("url", url)
                .option("user", userName)
                .option("password", passWd)
                .option("dbtable", to_table)
                .mode(SaveMode.Append)
                .save()
            } catch {
              case  e:SparkException=> logger.error("Exception in writing data to MySQL database by spark task {}:",e)
                System.exit(-1)
            }*/
          })
        }
      }.invoke(dt, dt1)

      spark.stop()
      logger.info("The task execution completed ......")
    } catch {
      case  e:SparkException=> {
        logger.error("DatabasesName={},TableName={},exception info {}",from_db,to_table,e.getMessage)
        logger.error("The task execution exception ......")
        System.exit(-1)
      }
    }



  }
}
