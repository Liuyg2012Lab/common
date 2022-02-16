package com.mingzhi.common.utils

import com.mingzhi.common.interf.IDate
import com.mingzhi.common.test.test1.{dt, spark}
import com.mingzhi.common.utils.TableUtils.isPartitionedByDt
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import java.util

import org.apache.spark.rdd.RDD

import scala.collection.{breakOut, mutable}
import scala.collection.mutable.ListBuffer

object TableUtils {
  private val logger = LoggerFactory.getLogger(TableUtils.getClass)

  /**
   * 业务表导入时先删除指定dt的分区
   */
  def delPartitions(spark: SparkSession, dt: String, dt1: String, to_db: String, to_table: String): Unit = {

    if (isPartitionedByDt(spark, to_db, to_table)) {

      new IDate {
        override def onDate(dt: String): Unit = {

          spark.sql(s"ALTER TABLE $to_db.$to_table  DROP IF EXISTS PARTITION (dt='${dt}')")

        }
      }.invoke(dt, dt1)

    } else {
      println(s"$to_table is not partitioned by dt,skip....")
    }
  }

  def isPartitionExists(spark: SparkSession, db: String, table: String, dt: String): Boolean = {
    spark.sql(s"use $db")

    val frame = spark.sql(s" show partitions $table")

    /**
     * +-------------+
     * |partition    |
     * +-------------+
     * |dt=2020-08-18|
     * |dt=2021-09-01|
     * |dt=2021-09-02|
     * |dt=2021-09-03|
     * |dt=2021-09-04|
     * |dt=2021-09-06|
     * +-------------+
     */
    frame.show(false)

    import scala.collection.JavaConversions._

    val dts: mutable.Seq[String] = frame
      .collectAsList()
      .map({ case Row(ct: String) => ct.split("=")(1): String })

    /**
     * ArrayBuffer(2020-08-18, 2021-09-01, 2021-09-02, 2021-09-03, 2021-09-04, 2021-09-06)
     */
    println(dts.toString())

    val isPartitionExist = dts.contains(dt)

    println("is partition exist:" + isPartitionExist)

    isPartitionExist
  }


  /**
   * 表t是否是按照dt进行分区的
   */
  private def isPartitionedByDt(spark: SparkSession, db: String, t: String): Boolean = {

    spark.sql(s"use $db")

    val frame = spark.sql(s" show create table $t")

    val createTableStatement = frame.select("createtab_stmt").collect().toSet.mkString(",")

    /**
     * [CREATE TABLE `t_uac_organization` (`id` STRING, `org_name` STRING, `parent_id` STRING, `sort` INT, `org_type` INT, `org_level` INT, `is_auth_scope` STRING, `parent_auth_scope_id` STRING, `status` INT, `icon_class` STRING, `create_id` STRING, `create_time` TIMESTAMP, `update_id` STRING, `update_time` TIMESTAMP, `version` INT, `dt` STRING)
     * USING parquet
     * OPTIONS (
     * `serialization.format` '1'
     * )
     * PARTITIONED BY (dt)
     * ]
     *
     *
     * [CREATE TABLE `ods_order`(`orderid` STRING, `ordercreatetime` STRING, `create_by` STRING, `home_address` STRING, `companyid` STRING, `areasid` STRING, `institutionid` STRING, `platfromfiledcode` STRING, `orderlargertype` STRING, `ordersecondtype` STRING, `orderthirdlytype` STRING, `serviceflowalias` STRING, `ordersource` STRING, `ordersourcename` STRING, `orderstatus` STRING, `orderstatusname` STRING, `actualhour` STRING, `urgent` STRING, `supervisenum` STRING, `reworknum` STRING, `importance` STRING, `dealuserids` STRING, `dealuserorgids` STRING, `comefrom` STRING, `rn` STRING, `floor_id` STRING, `flow_subsystem_code` STRING, `is_over_time` STRING)
     * PARTITIONED BY (`dt` STRING)
     * ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
     * WITH SERDEPROPERTIES (
     * 'serialization.format' = '1'
     * )
     * STORED AS
     * INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
     * OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
     * TBLPROPERTIES (
     * 'transient_lastDdlTime' = '1629711849',
     * 'last_modified_time' = '1629711660',
     * 'last_modified_by' = 'admin'
     * )
     * ]
     *
     *
     *
     * [CREATE TABLE `ads_iot_electricity_index2_120100lae` (`pk` STRING, `dt` STRING, `dh` STRING, `dm` STRING)
     * USING parquet
     * OPTIONS (
     * `serialization.format` '1'
     * )
     * PARTITIONED BY (dt, dh, dm)
     * ]
     *
     */

    //    println("createtab_stmt:" + createtab_stmt)

    createTableStatement.contains("PARTITIONED BY (`dt` STRING)") || createTableStatement.contains("PARTITIONED BY (dt")
  }

  /**
   * 判断某个表在某个库中是否存在，不考虑虚表
   */
  def tableExists(spark: SparkSession, to_db: String, to_table: String): Boolean = {
    spark.sql(s"use $to_db")
    val frame_table = spark.sql(s"show tables")

    /**
     * +--------+--------------------+-----------+
     * |database|           tableName|isTemporary|
     * +--------+--------------------+-----------+
     * |     iot|            ads_elec|      false|
     * |     iot|ads_iot_electrici...|      false|
     * |     iot|ads_iot_electrici...|      false|
     * |     iot|ads_iot_humiture_...|      false|
     * |        |t_iot_rule_meter_...|       true|
     * +--------+--------------------+-----------+
     */
    //    frame_table.show(100, false)

    /**
     * |-- database: string (nullable = false)
     * |-- tableName: string (nullable = false)
     * |-- isTemporary: boolean (nullable = false)
     */
    //    frame_table.printSchema()

    val str = frame_table
      .filter(r => {
        !r.getAs[Boolean]("isTemporary")
      })
      .select("tableName").collect().toSet.mkString(",")

    str.contains(s"[${to_table.toLowerCase}]")
  }

  def queryExecuteDate(spark: SparkSession, sql: String, dt: String): Array[String] = {
    val querySql = sql.replaceAll("do_date",dt);
    logger.info("querySql="+querySql)
    val frame_table = spark.sql(querySql).distinct().filter(" date is not null ")
    val data = frame_table.rdd.map(line => line.getAs("date").toString).distinct().collect().sorted
    data.foreach(date => {
      logger.info(s"$dt 修改过那一天的数据：" + date)
    })
    data
  }

  def formattedData(spark: SparkSession, frame: DataFrame): DataFrame = {
    var schema = frame.schema
    val fields: Array[StructField] = schema.fields
    var resultDataFrame = frame.rdd.mapPartitions(it => {
      var result = new ListBuffer[Row]()
      it.foreach(record => {
        var list = new ListBuffer[Any]()
        fields.foreach(StructField => {
          val clunnName = StructField.name
          val dateType = StructField.dataType
          dateType match {
            case _: ByteType => list.append(record.getAs(clunnName))
            case _: ShortType => list.append(record.getAs(clunnName))
            case _: IntegerType => list.append(record.getAs(clunnName))
            case _: LongType => list.append(record.getAs(clunnName))
            case _: BooleanType => list.append(record.getAs(clunnName))
            case _: FloatType => list.append(record.getAs(clunnName))
            case _: DoubleType => list.append(record.getAs(clunnName))
            case _: StringType => if (record.getAs(clunnName) == null) {
              list.append(record.getAs(clunnName))
            } else {
              list.append(record.getAs(clunnName).toString.replaceAll("\r", "").replaceAll("\n", ""))
            }
            case _: TimestampType => list.append(record.getAs(clunnName))
            case _: DateType => list.append(record.getAs(clunnName))
            case _: DecimalType => list.append(record.getAs(clunnName))
            case _ => list.append(record.getAs(clunnName))
          }
        })
        result.append(Row.fromSeq(list))
      })
      result.iterator
    })
    val targetDataFrame = spark.createDataFrame(resultDataFrame, schema)
    targetDataFrame
  }


  /**
   * 基于DataFrame创建对应的Mysql表
   */
  def createMysqlTableDDL(t_name: String, frame: DataFrame): String = {

    var sql = s"create table if not EXISTS $t_name(\n"

    frame.schema.foreach(f => {

      val name = f.name
      var tp = "varchar(64)"
      val comment: Option[String] = f.getComment()

      tp = f.dataType match {
        case _: StringType => "varchar(256)"
        case _: DateType | _: TimestampType => "varchar(128)"
        case _: IntegerType => "int"
        case _: LongType => "bigint"
        case _: DoubleType | _: FloatType | _: DecimalType => "decimal(16,4)"
        case _ => "text"
      }

      sql = sql + s"$name $tp COMMENT '${comment.getOrElse("")}',\n"
    })
    sql.substring(0, sql.length - 2) + ") ENGINE=InnoDB DEFAULT CHARSET=utf8"
  }

  /**
   * 基于DataFrame创建对应的样例类
   */
  def createCaseClass(clzName: String, frame: DataFrame): String = {

    var sql = s"case class $clzName(\n"

    frame.schema.foreach(s => {

      val name = s.name
      var tp = "String"


      tp = s.dataType.typeName match {
        case "string" | "date" => "String"
        case "integer" => "Integer"
        case "double" | "float" => "Double"
        case "long" => "Long"
        case "array" => "Array[Any]"
        case _ => throw new IllegalArgumentException(s"未知的数据类型:${s.dataType.typeName}")
      }
      sql = sql + s"var $name:$tp,\n"
    })
    sql.substring(0, sql.length - 2) + "\n)"
  }


  /**
   * DataFrame的字段添加注释
   */
  def addComment(spark: SparkSession, f: DataFrame, commentMap: java.util.Map[String, String]): DataFrame = {

    val schema = f.schema.map(s => {
      s.withComment(commentMap.getOrDefault(s.name, ""))
    })

    spark.createDataFrame(f.rdd, StructType(schema))

  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtils.getBuilder.master("local[*]").getOrCreate()

    //        isPartitionExists(spark, "paascloud", "dwd_order_info_abi", "2021-09-07")

    val f = spark.sql(
      """
        |select * from hr.dwd_staff_info where dt='9999-12-31'
        |""".stripMargin)
    //      .withColumn("money", lit(0.01d))
    //      .withColumn("money1", lit(0.01f))
    //      .withColumn("money1", lit(100000L))

    f.show(false)


    val fields: Array[StructField] = f.schema.fields

    fields.foreach(f => {


      println("name:" + f.name + ",dataType:" + f.dataType + "," + "metadata:" + f.metadata + ",comment:" + f.getComment() + ",toDDL:" + f.toDDL)

    })


    //    f.schema.foreach(s => {
    //
    //
    //      println("name:" + s.name + ",typeName:" + s.dataType.typeName + ",metadata:" + s.metadata + ",comment:" + s.getComment().getOrElse(""))
    //    })


    println(createMysqlTableDDL("person", f))
    println(createCaseClass("A", f))

  }
}

