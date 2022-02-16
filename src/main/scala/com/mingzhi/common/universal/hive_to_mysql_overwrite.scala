package com.mingzhi.common.universal

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Date

import com.mingzhi.common.interf.IDate
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 从数仓同步表到mysql的类，用于覆盖导入
  */
object hive_to_mysql_overwrite {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    var dt = "2021-01-24"
    var dt1 = "2021-01-24"

    var dbServer = "192.168.0.169"
    var from_db = "paascloud_ewo"
    var from_table = "dwd_t_uac_organization_info"
    var to_db = "paascloud_ewo"
    var to_table = "ads_organization_info"
    var userName = "mzadmin"
    var passWd = "Mz@123456"

    val builder = SparkSession.builder()

    builder
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .enableHiveSupport()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      builder.master("local[*]")
    } else {
      dbServer = args(0)
      from_db = args(1)
      from_table = args(2)
      to_db = args(3)
      to_table = args(4)
      userName = args(5)
      passWd = args(6)
      dt = args(7)
      dt1 = args(8)
    }
    builder.appName(from_table)
    val spark = builder.getOrCreate()

    val driver = "com.mysql.jdbc.Driver"
    val url = s"jdbc:mysql://$dbServer:3306/$to_db?tinyInt1isBit=false&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000"


    new IDate {
      override def onDate(dt: String): Unit = {

        val frame = spark.sql(
          s"""
             |select
             |'${DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")}' as last_update_time,
             |* from
             |$from_db.$from_table where dt='$dt'
             |""".stripMargin)

        frame.show(true)
                println(frame.count())


        frame.rdd.foreachPartition(it => {

          Class.forName(driver)
          val conn: Connection = DriverManager.getConnection(url, userName, passWd)

          /**
            * 清空表的所有数据
            */
          val statement: PreparedStatement = conn.prepareStatement(s"DELETE FROM $to_table WHERE dt between '2000-00-00' and '9999-99-99'")

          println(s"删除所有数据...")
          statement.execute()
        })

        println(s"导入${dt}新的数据")
        frame.write
          .format("jdbc")
          .option("url", url)
          .option("user", userName)
          .option("password", passWd)
          .option("dbtable", to_table)
          .mode(SaveMode.Append)
          .save()

      }
    }.invoke(dt, dt1)

    spark.stop()

  }
}
