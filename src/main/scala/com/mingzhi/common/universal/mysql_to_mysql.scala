package com.mingzhi.common.universal

import com.mingzhi.ConfigUtils
import com.mingzhi.common.interf.MySaveMode
import com.mingzhi.common.utils.{SinkUtil, SourceUtil, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, PreparedStatement}

/**
 * mysql之间的数据同步
 * 支持一个库内的多表批量同步
 * 支持目标表增加前缀
 * TODO 支持自动建表(基于源表ddl)
 */
object mysql_to_mysql {

  def main(args: Array[String]): Unit = {


    var host1 = ConfigUtils.WFS_MYSQL_HOST
    var db1 = "paascloud_wfs"
    var tables = "ads_order_exception_cube"
    var userName1 = ConfigUtils.WFS_MYSQL_USERNAME
    var passWd1 = ConfigUtils.WFS_MYSQL_PWD

    var host2 = ConfigUtils.WFS_MYSQL_HOST
    var db2 = "paascloud_wfs"
    var userName2 = ConfigUtils.WFS_MYSQL_USERNAME
    var passWd2 = ConfigUtils.WFS_MYSQL_PWD

    var prefix = "wfs_"

    System.setProperty("HADOOP_USER_NAME", "root")

    val builder = SparkUtils.getBuilder

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")
    } else {
      host1 = args(0)
      db1 = args(1)
      tables = args(2) // t_a,t_b,t_c
      userName1 = args(3)
      passWd1 = args(4)

      host2 = args(5)
      db2 = args(6)
      userName2 = args(7)
      passWd2 = args(8)
      prefix = args(9)
    }

    val tableArr = tables.split(",")

    val spark = builder.appName(tables + "_to_...").getOrCreate()

    tableArr.foreach(t => {

      process(spark, host1, db1, t, userName1, passWd1,
        host2, db2, prefix + t, userName2, passWd2)
    })


    spark.stop()

  }

  def process(spark: SparkSession,
              host1: String, from_db: String, from_table: String, userName1: String, passWd1: String,
              mysql_host2: String, to_db: String, to_table: String, userName2: String, passWd2: String): Unit = {

    println(s"同步$from_table...............")


    //    val url = s"jdbc:mysql://${host1}:3306/${from_db}?tinyInt1isBit=false&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000"
    //    val frame: DataFrame = spark.read.format("jdbc")
    //      .options(Map("url" -> url, "user" -> userName1, "password" -> passWd1, "dbtable" -> s"(SELECT * FROM ${from_table}) t"))
    //      .load()

    var host = host1
    var port = "3306"

    if (host.split(":").length >= 2) {
      port = host.split(":")(1)
      host = host.split(":")(0)
    }


//    val url = s"jdbc:mysql://${host}:${port}/${from_db}?useUnicode=true&characterEncoding=UTF-8&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000&useSSL=false"
//
//    var conn: Connection = null
//    var prepareStatement: PreparedStatement = null
//
//
//    prepareStatement = conn.prepareStatement(s"SHOW CREATE TABLE $from_table")
//
//    prepareStatement.execute()

    /**
     * 表数据,无comment信息
     */
    val frame = SourceUtil.sourceAllFromMysql(spark, host, port, from_db, from_table, userName1, passWd1)


    frame.show(false)

    frame.printSchema()



    SinkUtil.sink_to_mysql(mysql_host2, to_db, to_table, userName2, passWd2, frame, 3, MySaveMode.OverWriteAllTable, null)
    //    SinkUtil.sink_to_mysql2(spark, mysql_host2, to_db, to_table, userName2, passWd2, frame, 3, MySaveMode.OverWriteAllTable, null)

  }
}
