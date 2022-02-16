package com.mingzhi.common.universal

import com.mingzhi.common.interf.{IDate, LoadStrategy}
import com.mingzhi.common.utils.SinkUtil
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.Properties

/**
 * clickhouse 作为数据源
 * hive  作为目标数据（ck to hive）
 *
 * @parameter ck_host 主机/服务器ip,端口默认8123，如不为默认端口，则指定 "ip:port" 的方式传参
 * @parameter ck_db  数据源库名
 * @parameter ck_table  数据源表名
 * @parameter hive_db   目标库名
 * @parameter hive_table  目标表名
 * @parameter dt ,dt1  开始时间  -结束时间
 * @parameter ct_or_ut_time 创建时间或者更新时间
 * */

object common_ck_to_hive {

  //===========================params define=====================================
  /**
   * ip or "ip:port"
   */
  private var ck_host: String = ""
  private var ck_db = ""
  private var ck_table = ""
  private var hive_db = ""
  private var hive_table = ""

  private var dt: String = ""
  private var dt1: String = ""

  /**
   * "ct_or_ut_time" or ""
   */
  private var ct_or_ut_time = ""
  //================================================================

  private var ck_port = "8123"


  /**
   * 默认抽数策略为抽新增
   * 实际根据程序传参参数确定策略
   */
  private var load_strategy: LoadStrategy.Value = LoadStrategy.NEW

  def main(args: Array[String]): Unit = {
    //根据规范，用trnuser用户创建hive表
    System.setProperty("HADOOP_USER_NAME", "trnuser")

    val builder = SparkSession.builder()

    //本地或者集群
    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")

      ck_host = "10.248.31.22:8122"
      ck_db = "ept"
      ck_table = "ept_SAS_stock_detail_rpt_all_day"
      hive_db = "ods"
      hive_table = "ods_sas_stock_detail_rpt_all_day_test5"

      ct_or_ut_time = "Month"

      dt = "2020-06-14"
      dt1 = "2020-06-15"


    } else {
      ck_host = args(0)
      ck_db = args(1)
      ck_table = args(2)
      hive_db = args(3)
      hive_table = args(4)

      ct_or_ut_time = args(5)

      dt = args(6)
      dt1 = args(7)
    }

    /**
     * 参数解析，确定抽数策略
     */

    if (ck_host.split(":").length > 1) {
      ck_port = ck_host.split(":")(1)
      ck_host = ck_host.split(":")(0)
    }

    if ("".equals(ct_or_ut_time)) {

      load_strategy = LoadStrategy.ALL

    } else {

      load_strategy = LoadStrategy.NEW

    }

    builder
      .appName("ck_to_hive")
      //      .config("hive.exec.scratchdir", s"hdfs://nameservice1:8020//tmp/hive")
      .enableHiveSupport()

    val spark: SparkSession = builder.getOrCreate()

    val ckDriver = "ru.yandex.clickhouse.ClickHouseDriver"
    val ckUrl = s"jdbc:clickhouse://${ck_host}:${ck_port}/${ck_db}"
    val pro = new Properties()
    pro.put("driver", ckDriver)

    //    val frame_ck: DataFrame = spark.read.jdbc(ckUrl, ck_db + "." + ck_table, pro)
    //      .persist(StorageLevel.MEMORY_AND_DISK)
    //    frame_ck.show(false)

    val frame_ck = createTesFrame(spark)

    new IDate {
      override def onDate(dt: String): Unit = {

        val frame_result = process(spark, frame_ck, dt, load_strategy, ct_or_ut_time)

        SinkUtil.sink_to_hive(dt, spark, frame_result, hive_db, hive_table)

      }
    }.invoke(dt, dt1)

    spark.stop()

  }

  def process(spark: SparkSession, frame_ck: DataFrame, dt: String, load_strategy: LoadStrategy.strategy, ct_or_ut_time: String): DataFrame = {

    var frame: DataFrame = null

    if (load_strategy == LoadStrategy.NEW) {

      frame_ck.createOrReplaceTempView("t")

      /**
       * dt是我们框架的标准格式 yyyy-MM-dd
       * 但是被导入的数据时间格式有可能是yyyyMMdd
       * 根据公司具体情况，有什么格式，这里都进行适配
       */
      val dt1 = dt.replace("-", "")


      frame = spark.sql(
        s"""
           |select '$dt' as dt,*
           |from t
           |where $ct_or_ut_time='$dt' or $ct_or_ut_time='$dt1' or date_format($ct_or_ut_time,'yyyy-MM-dd')='$dt'
           |""".stripMargin)

    } else if (load_strategy == LoadStrategy.ALL) {

      frame = frame_ck.withColumn("dt", lit(dt))

    }

    frame
  }

  def createTesFrame(spark: SparkSession): DataFrame = {

    spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L, "20210411"),
      ("ming", 20, 15552211521L, "20210412"),
      ("lucy", 20, 15552211521L, "20210413"),
      ("hong", 19, 13287994007L, "2021-04-12 00:00:05"),
      ("zhi", 21, 15552211523L, "2021-04-12")
    )).toDF("name", "age", "phone", "Month")
      .persist(StorageLevel.MEMORY_AND_DISK)
  }
}
