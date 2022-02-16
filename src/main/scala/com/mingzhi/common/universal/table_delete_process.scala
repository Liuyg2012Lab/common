package com.mingzhi.common.universal

import com.mingzhi.common.utils.{SparkUtils, TableUtils}
import org.apache.spark.sql.SparkSession

/**
 * 表删除任务
 */
object table_delete_process {

  private var hive_db: String = "iot"

  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "root")

    val builder = SparkUtils.getBuilder

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")
    } else {
      hive_db = args(0)
    }

    val spark: SparkSession = builder.appName("delete_tables").getOrCreate()

    delete_all_table(spark, hive_db)

    spark.stop()

  }


  private def delete_all_table(spark: SparkSession, hive_db: String): Unit = {

    spark.sql(s"use $hive_db")
    val frame_table = spark.sql(s"show tables")

    frame_table.show(100, false)
    frame_table.printSchema()

    frame_table
      .filter(r => {
        !r.getAs[Boolean]("isTemporary")
      })
      .select("tableName").collect().foreach(r => {
      //r:[ads_order_topn]
      val table = r.toString().replace("[", "").replace("]", "")

      println("delete table:" + table)

      if (TableUtils.tableExists(spark, hive_db, table)) {

        spark.sql(
          s"""
             |
             |drop table $table
             |
             |""".stripMargin)

      }
    })
  }
}
