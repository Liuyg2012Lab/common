package com.mingzhi.common.sqlserver

import com.mingzhi.common.utils.TableUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * 把数据从SQL Server 抽取到Hive
 */
object SqlServerToHive {
  private val logger = LoggerFactory.getLogger(SqlServerToHive.getClass)
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    if(args.length<7){
      println("parameter is : [dbServer,from_db,from_table,to_db,to_table,username,password]")
      System.exit(-1)
    }

    val dbServer = args(0)
    val from_db = args(1)
    val from_table = args(2)
    val to_db=args(3)
    val to_table = args(4)
    val username = args(5)
    val password = args(6)
    val dt = args(7)
    val dt1 = args(8)
    val createTime = args(9)

    val sparkBuilder = SparkSession
      .builder()
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName("Import_"+from_db+"_"+from_table)
      .enableHiveSupport()

    val spark: SparkSession = sparkBuilder.getOrCreate()

    val expression = s"${createTime} between '${dt} 00:00:00' and '${dt1} 23:59:59'"
    val url = s"jdbc:sqlserver://$dbServer:1433;databaseName=$from_db"

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", url)
      .option("user", s"$username")
      .option("password", s"$password")
      .option("dbtable", s"(SELECT * FROM ${from_table} where $expression) t")
      .load()

    jdbcDF.createOrReplaceTempView("temp_sqlserver_table")

    var frame = spark.sql(
      s"""
         |select date_format(${createTime},'yyyy-MM-dd') as dt,*
         |from temp_sqlserver_table
         |""".stripMargin)
    frame = TableUtils.formattedData(spark, frame)

    if (TableUtils.tableExists(spark, to_db, to_table)) {
      TableUtils.delPartitions(spark, dt, dt1, to_db, to_table)
      logger.info(s"${to_table}表存在,删除${dt}到${dt1}分区的数据")
      /**
        * 1.差集
        */
      val diff = frame.columns.toSet &~ spark.sql(
        s"""
           |
           |select * from ${to_db}.${to_table} where dt='9999-99-99'
           |
        """.stripMargin).columns.toSet
      /**
        * 2.差集是新添加字段,新加字段数据舍弃
        */
      diff.foreach(x => frame = frame.drop(x))
    } else {
      logger.info(s"${to_table}表不存在")
    }

    frame
     .coalesce(10)
     .write
     .format("parquet")
     .mode(SaveMode.Append)
      .partitionBy("dt")
     .saveAsTable(s"${to_db}.$to_table")


     spark.stop()
}
}
