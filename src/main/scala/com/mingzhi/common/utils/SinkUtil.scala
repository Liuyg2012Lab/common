package com.mingzhi.common.utils

import com.mingzhi.common.interf.MySaveMode
import com.mingzhi.common.interf.MySaveMode.MySaveMode
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement}

object SinkUtil {
  private val logger = LoggerFactory.getLogger(SinkUtil.getClass)

  def sink_to_mysql(host0: String, db: String, target_table: String, userName: String, pwd: String, frame_result: DataFrame, partitions: Int, mySaveMode: MySaveMode, dt: String): Unit = {

    var host = host0
    var port = "3306"

    if (host0.split(":").length >= 2) {
      port = host0.split(":")(1)
      host = host0.split(":")(0)
    }

    val url = s"jdbc:mysql://${host}:${port}/${db}?useUnicode=true&characterEncoding=UTF-8&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000&useSSL=false"

    var conn: Connection = null
    var prepareStatement: PreparedStatement = null

    try {
      conn = DriverManager.getConnection(url, userName, pwd)
      val statement = conn.createStatement()

      /**
       * 如果Mysql表不存在,自动建表
       */
      val ddl = TableUtils.createMysqlTableDDL(target_table, frame_result)
      println(ddl)
      statement.execute(ddl)
      DbUtils.release(null, statement)

      conn.setAutoCommit(false)
      if (mySaveMode == MySaveMode.OverWriteAllTable) {

        /**
         * in this time, the arg of dt can be set to null
         */
        prepareStatement = conn.prepareStatement(s"DELETE FROM $target_table WHERE 1=1")

      } else if (mySaveMode == MySaveMode.OverWriteByDt) {

        prepareStatement = conn.prepareStatement(s"DELETE FROM $target_table WHERE dt='$dt'")

      }
      prepareStatement.execute()

    } catch {
      case e: Exception => logger.error("MySQL connection exception {}:", e)
        if (conn != null) {
          conn.rollback()
        }
        DbUtils.release(conn, prepareStatement)
        System.exit(-1)
    } finally {
      if (null != conn && !conn.isClosed) {
        conn.commit()
      }
      DbUtils.release(conn, prepareStatement)
    }


    try {
      frame_result
        .filter(x => {

          var flag = false

          if (mySaveMode == MySaveMode.OverWriteAllTable) {

            flag = true

          } else if (mySaveMode == MySaveMode.OverWriteByDt) {

            flag = dt.equalsIgnoreCase(x.getAs[String]("dt"))
          }

          flag

        })
        .repartition(partitions)
        .write
        .format("jdbc")
        .option("url", url)
        .option("user", userName)
        .option("password", pwd)
        .option("dbtable", target_table)
        .mode(SaveMode.Append)
        .save()
    } catch {
      case e: SparkException => logger.error("Exception in writing data to MySQL database by spark task {}:", e)
        System.exit(-1)
    }
  }

  /**
   * 只导入dt该天对应的数据
   */
  def sink_to_hive(dt: String, spark: SparkSession, frame_result: DataFrame, hive_db: String, hive_table: String): Unit = {
    sink_to_hive(dt, spark, frame_result, hive_db, hive_table, "parquet")
  }

  def sink_to_hive(dt: String, spark: SparkSession, frame_result: DataFrame, hive_db: String, hive_table: String, format: String): Unit = {
    sink_to_hive(dt, spark, frame_result, hive_db, hive_table, format, MySaveMode.OverWriteByDt)
  }

  def sink_to_hive(dt: String, spark: SparkSession, frame_result: DataFrame, hive_db: String, hive_table: String, format: String, mySaveMode: MySaveMode): Unit = {
    sink_to_hive(dt, spark, frame_result, hive_db, hive_table, format, mySaveMode, 3)
  }

  def sink_to_hive(dt: String, spark: SparkSession, frame_result: DataFrame, hive_db: String, hive_table: String, format: String, mySaveMode: MySaveMode, partitions: Int): Unit = {
    sink_to_hive(dt, spark, frame_result, hive_db, hive_table, format, mySaveMode, partitions, null)
  }

  def sink_to_hive(dt: String, spark: SparkSession, frame_result: DataFrame, hive_db: String, hive_table: String, format: String, mySaveMode: MySaveMode, partitions: Int, comment: java.util.Map[String, String]): Unit = {

    try {
      var frame = TableUtils.formattedData(spark, frame_result.filter(x => {
        dt.equalsIgnoreCase(x.getAs[String]("dt"))
      }))

      var diff: Set[String] = Set()
      var diff1: Set[String] = Set()

      if (TableUtils.tableExists(spark, hive_db, hive_table)) {

        TableUtils.delPartitions(spark, dt, dt, hive_db, hive_table)

        println(s"${hive_db}.${hive_table}表存在,删除${dt}分区的数据")

        diff = frame.columns.toSet &~ spark.sql(
          s"""
            select * from $hive_db.$hive_table where dt='9999-99-99'
            """.stripMargin).columns.toSet

        diff1 = spark.sql(
          s"""
            select * from $hive_db.$hive_table where dt='9999-99-99'
            """.stripMargin).columns.toSet diff frame.columns.toSet


      } else {
        println(s"${hive_table}表不存在,write directly")
      }

      /**
       * 业务表新增的字段,补充
       */
      diff.foreach(x => {

        spark.sql(
          s"""
             |ALTER TABLE ${hive_db}.${hive_table} ADD COLUMNS(${x} STRING)
             |""".stripMargin)

      }
      )

      /**
       * 业务表减少的字段,用空串占位,但不能保证hive表Schema的一致
       */
      for (elem <- diff1) {
        println("add col " + elem)
        frame = frame.withColumn(elem, lit(""))
      }


      /**
       * 处理表字段的注释信息
       */
      if (null != comment && !comment.isEmpty) {
        frame = TableUtils.addComment(spark, frame, comment)
      }

      /**
       * 分区处理
       * partition指定为0的时候,按照数据量分区,3w/分区
       * 大于0的时候,使用指定数值进行分区
       */
      var parti = 3
      if (partitions == 0) {
        parti = (frame.count() / 30000).toInt
      } else {
        parti = partitions
      }


      if (parti > frame.rdd.getNumPartitions) {
        frame
          .repartition(parti)
          .write
          .format(format)
          .mode(if (mySaveMode == MySaveMode.OverWriteByDt) SaveMode.Append else SaveMode.Overwrite)
          .partitionBy("dt")
          .saveAsTable(s"$hive_db.$hive_table")
      } else {

        frame
          .coalesce(parti)
          .write
          .format(format)
          .mode(if (mySaveMode == MySaveMode.OverWriteByDt) SaveMode.Append else SaveMode.Overwrite)
          .partitionBy("dt")
          .saveAsTable(s"$hive_db.$hive_table")
      }
    } catch {
      case e: SparkException => logger.error("The spark task failed to write data to hive database  : {}", e)
      case e2: java.lang.Exception => logger.error("The spark task failed to write data to hive database of error2  : {}", e2)
      case _: java.lang.Exception => logger.error("The spark task failed to write data to hive database,未知异常")
        System.exit(-1)
    }
  }
}
