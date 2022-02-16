package com.mingzhi.common.universal

import com.mingzhi.common.utils.SparkUtils
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
 * 从导入策略为新增和变化的表中提取数据产生了变化的dt
 */
object get_changed_dt {

  def main(args: Array[String]): Unit = {

    var hive_db = "paascloud"
    var hive_table = "ods_order"
    var create_time = "ordercreatetime"
    var dt = "2020-08-12"

    System.setProperty("HADOOP_USER_NAME", "root")
    val builder = SparkUtils.getBuilder

    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      builder.master("local[*]")
    } else {

      hive_db = args(0)
      hive_table = args(1)
      create_time = args(2)
      dt = args(3)

    }
    val spark = builder.appName(getClass.getSimpleName).getOrCreate()

    val dts = get_changed_dt(spark, hive_db, hive_table, create_time, dt)

    dts.foreach(println(_))

    spark.stop()
  }


  def get_changed_dt(spark: SparkSession, db_name: String, table_name: String, creat_time: String, dt: String): mutable.Buffer[String] = {
    val frame_dt = spark.sql(
      s"""
         |select
         |ct
         |from (
         |        select
         |                date_format($creat_time, 'yyyy-MM-dd') as ct
         |        from
         |                $db_name.$table_name
         |        where
         |               dt = '$dt'
         |) t group by ct
         |
      """.stripMargin)

    frame_dt.show(false)


    import scala.collection.JavaConversions._

    frame_dt
      .collectAsList()
      .map({ case Row(ct: String) => ct: String })


  }
}
