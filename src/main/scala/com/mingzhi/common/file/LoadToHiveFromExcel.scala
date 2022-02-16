package com.mingzhi.common.file

import com.mingzhi.common.utils.{SparkUtils, TableUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection.mutable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
 * 导新增
 * 如工单轨迹表 tbwork_order_track_record
 */
object LoadToHiveFromExcel {
  private val logger = LoggerFactory.getLogger(LoadToHiveFromExcel.getClass)
  private var diff: Set[String] = Set()

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    var path=""
    var to_db = ""
    var to_table = ""
    var clumn_names="" //A,B,C
    var dt = "2020-07-15"
    var dt1 = "2020-07-15"
    var createTime = "create_time"
    var fileType="xlsx"

    val builder = SparkSession.builder()
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .appName(to_table)
      .enableHiveSupport()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      builder.master("local[*]")
      path="E:\\Documents\\工作日志\\钉钉工作日志.xlsx"
      to_db = "uac_user"
      to_table = "ding_work_log"
      clumn_names="user_id,name,org_name,fill_in_time,last_modified_time,period_time,week_work_content,today_work_content,support_content,nextday_work_content,yourself_summary,remarks,support_content4,addres,comment,even,support_content8,read,unread,read_rate" //A,B,C
      dt = "2020-07-15"
      dt1 = "2020-07-15"
      createTime = "fill_in_time"
      fileType ="xlsx"
    } else {
      path=args(0)
      to_db = args(1)
      to_table = args(2)
      clumn_names=args(3)
      dt = args(4)
      dt1 = args(5)
      createTime = args(6)
      fileType = args(7)
      logger.info("path="+path)
      logger.info("to_db="+to_db)
      logger.info("to_table="+to_table)
      logger.info("clumn_names="+clumn_names)
      logger.info("fileType="+fileType)
    }
    val spark:SparkSession = builder.getOrCreate()
    var readDF = spark.emptyDataFrame
    var fileFormat="com.databricks.spark.csv"
    if(fileType.contains("xls") || fileType.contains("xlsx")){
      fileFormat="com.crealytics.spark.excel"
      readDF = spark.read.format(fileFormat)
        .option("header", true)
        .option("treatEmptyValuesAsNulls", true) // 空值处理为null
        .option("inferSchema",true)      //这是自动推断属性列的数据类型。
        .schema(createStructType(clumn_names))
        .load(path)
    }else{
      readDF = spark.read.format(fileFormat)
        .option("header", true)
        .option("inferSchema", true)
        .option("nullValue", "\\N")
        .schema(createStructType(clumn_names))
        .load(path)
    }
    readDF = TableUtils.formattedData(spark, readDF)
    readDF.show()
    readDF.createOrReplaceTempView("temp_excel")
    var frame = spark.sql(
      s"""
         |
         |select date_format(regexp_replace(regexp_replace($createTime,'[年|月]','-'),'日',''),'yyyy-MM-dd') as dt,* from temp_excel
         |
         |""".stripMargin)

    if (TableUtils.tableExists(spark, to_db, to_table)) {
      TableUtils.delPartitions(spark, dt, dt1, to_db, to_table)
      println(s"${to_table}表存在,删除${dt}到${dt1}分区的数据")

      diff = frame.columns.toSet &~ spark.sql(
        s"""
           |
           |select * from ${to_db}.${to_table} where dt='9999-99-99'
           |
        """.stripMargin).columns.toSet
    } else {
      println(s"${to_table}表不存在")
    }

    diff.foreach(x => frame = frame.drop(x))

    println("frame result show==================================")
    frame.show(true)

    frame
      .coalesce(3)
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("dt")
      .saveAsTable(s"${to_db}.${to_table}")

    spark.stop()

  }


  def createStructType(clumn_names:String) = {
    val fields = mutable.ArrayBuffer[StructField]()
    clumn_names.split(",").foreach(f=>{
      fields += DataTypes.createStructField(f, DataTypes.StringType, true)
    })
    DataTypes.createStructType(fields.toArray)
  }
}
