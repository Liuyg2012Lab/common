package com.mingzhi.common.universal

import com.mingzhi.common.interf.MySaveMode
import com.mingzhi.common.utils.{SinkUtil, SparkUtils, StringUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/**
 * 统统全量
 */
object sqlserver_to_hive {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    if (args.length < 7) {
      println("parameter is : [dbServer,from_db,from_table,username,password,to_db,to_table,dt]")
      System.exit(-1)
    }

    var host = args(0)
    val db = args(1)
    val tables = args(2)
    val username = args(3)
    val password = args(4)
    val hive_db = args(5)
    val hive_tables = if (StringUtils.isBlank(args(6))) tables else args(6)
    val dt = args(7)

    StringUtil.assertNotBlank(host, "host can not be null")
    StringUtil.assertNotBlank(db, "db can not be null")
    StringUtil.assertNotBlank(tables, "tables can not be null")
    StringUtil.assertNotBlank(username, "username can not be null")
    StringUtil.assertNotBlank(password, "password can not be null")
    StringUtil.assertNotBlank(hive_db, "hive_db can not be null")

    if (dt.length != "yyyy-MM-dd".length) {
      throw new IllegalArgumentException("dt格式必须为yyyy-MM-dd")
    }

    if (tables.split(",").length != hive_tables.split(",").length) {
      throw new IllegalArgumentException("源表和目标表数量不一致...")
    }

    var port = "1433"

    if (host.split(":").length >= 2) {
      port = host.split(":")(1)
      host = host.split(":")(0)
    }

    val spark: SparkSession = SparkUtils.getBuilder.appName(s"import $tables from sqlserver to hive").getOrCreate()

    tables.split(",").zip(hive_tables.split(",")).foreach(r => {

      val tableSource = r._1
      val tableTarget = r._2

      val jdbcDF = spark.read
        .format("jdbc")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("url", s"jdbc:sqlserver://$host:$port;databaseName=$db")
        .option("user", s"$username")
        .option("password", s"$password")
        .option("dbtable", tableSource)
        .load()
        .withColumn("dt", lit(dt))

      jdbcDF.show()


      SinkUtil.sink_to_hive(
        dt
        , spark
        , jdbcDF
        , hive_db
        , tableTarget
        , "orc"
        , MySaveMode.OverWriteAllTable
        , 1,
        null
      )

    })

    spark.stop()
  }
}
