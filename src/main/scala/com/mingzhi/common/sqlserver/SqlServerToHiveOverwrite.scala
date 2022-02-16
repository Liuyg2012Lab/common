package com.mingzhi.common.sqlserver

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 把数据从SQL Server 抽取到Hive
 */
object SqlServerToHiveOverwrite {
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

    val sparkBuilder = SparkSession
      .builder()
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .appName("Import_"+from_db+"_"+from_table)
      .enableHiveSupport()

    val spark: SparkSession = sparkBuilder.getOrCreate()

    val url = s"jdbc:sqlserver://$dbServer:1433;databaseName=$from_db"

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", url)
      .option("user", s"$username")
      .option("password", s"$password")
      .option("dbtable", from_table)
      .load()

     //jdbcDF.show()


    jdbcDF
     .coalesce(10)
     .write
     .format("parquet")
     .mode(SaveMode.Overwrite)
     .saveAsTable(s"${to_db}.$to_table")


     spark.stop()
}
}
