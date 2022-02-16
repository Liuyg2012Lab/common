package com.mingzhi.common.universal

import com.mingzhi.common.interf.MySaveMode
import com.mingzhi.common.utils.{SinkUtil, SparkUtils, StringUtil}
import org.apache.spark.sql.SparkSession

/**
 * 抽一些基础表,不经过hive
 */
object sqlserver_to_mysql {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")


    var host = args(0)
    val db = args(1)
    val tables = args(2)
    val username = args(3)
    val password = args(4)

    val host2 = args(5)
    val db2 = args(6)
    val userName2 = args(7)
    val passWd2 = args(8)
    val prefix = args(9)

    StringUtil.assertNotBlank(host, "host can not be null")
    StringUtil.assertNotBlank(db, "db can not be null")
    StringUtil.assertNotBlank(tables, "tables can not be null")
    StringUtil.assertNotBlank(username, "username can not be null")
    StringUtil.assertNotBlank(password, "password can not be null")
    StringUtil.assertNotBlank(host2, "host2 can not be null")
    StringUtil.assertNotBlank(db2, "db2 can not be null")
    StringUtil.assertNotBlank(userName2, "userName2 can not be null")
    StringUtil.assertNotBlank(passWd2, "passWd2 can not be null")


    var port = "1433"

    if (host.split(":").length >= 2) {
      port = host.split(":")(1)
      host = host.split(":")(0)
    }

    val spark: SparkSession = SparkUtils.getBuilder.appName(s"import $tables from sqlserver to mysql").getOrCreate()

    tables.split(",").foreach(t => {


      val frame = spark.read
        .format("jdbc")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("url", s"jdbc:sqlserver://$host:$port;databaseName=$db")
        .option("user", s"$username")
        .option("password", s"$password")
        .option("dbtable", t)
        .load()

      frame.show()

      SinkUtil.sink_to_mysql(host2, db2, prefix + t, userName2, passWd2, frame, 3, MySaveMode.OverWriteAllTable, null)


    })

    spark.stop()
  }
}
