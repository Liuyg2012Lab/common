package com.mingzhi.common.universal

import com.mingzhi.common.utils.{SparkUtils, TableUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**
 * 分区表分区数据清理任务
 */
object table_clean_process {

  private var hive_dbs: String = "paascloud"
  private var hive_tables: String = "t_uac_organization"
  private var dt: String = "2021-09-01"
  private var dt1: String = "2021-09-01"

  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "root")

    val builder = SparkUtils.getBuilder

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")
    } else {
      hive_dbs = args(0)
      hive_tables = args(1)
      dt = args(2)
      dt1 = args(3)
    }

    val spark: SparkSession = builder.appName("clean_process").getOrCreate()


    if ("all".equalsIgnoreCase(hive_dbs)) {

      val builder = new StringBuilder()


      val frame_db = spark.sql("show databases").select("databaseName")

      frame_db.show(false)

      frame_db.collect().foreach(db => {

        builder.append(db.toString().replace("[", "").replace("]", ","))

      })

      /**
       * dbs:contract,customer_active,default,inspection,iot,paascloud,paascloud_ams,paascloud_bfo,paascloud_crm,paascloud_ewo,paascloud_iao,paascloud_ins,paascloud_laf,paascloud_mws,paascloud_rsv,paascloud_sds,paascloud_tcm,paascloud_tcs,paascloud_tms,paascloud_wfs,passcloud_rsv,tbwaste_manage,upkeep_equipment,
       */
      println("dbs:" + builder.toString())

      hive_dbs = builder.toString()

    }

    hive_dbs.split(",").foreach(db => {

      if (StringUtils.isNotBlank(db)) {

        if ("all".equalsIgnoreCase(hive_tables)) {

          clean_all_table(spark, db)

        } else {

          hive_tables.split(",").foreach(t => {

            clean_the_table(spark, db, t)
          })

        }
      }
    })
    spark.stop()

  }

  private def clean_the_table(spark: SparkSession, hive_db: String, table: String): Unit = {

    println("clean_the_table======>:" + hive_db + "." + table)

    spark.sql(s"use $hive_db")
    if (TableUtils.tableExists(spark, hive_db, table)) {

      TableUtils.delPartitions(spark, dt, dt1, hive_db, table)

    }
  }

  private def clean_all_table(spark: SparkSession, hive_db: String): Unit = {

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

      println("clean table:" + hive_db + "." + table)

      if (TableUtils.tableExists(spark, hive_db, table)) {

        TableUtils.delPartitions(spark, dt, dt1, hive_db, table)

      }
    })
  }
}
