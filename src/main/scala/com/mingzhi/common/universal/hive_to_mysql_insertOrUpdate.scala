package com.mingzhi.common.universal

import java.sql.{Connection, DriverManager, Timestamp}
import java.util.Date

import com.mingzhi.DatabaseUtil
import com.mingzhi.common.interf.IDate
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * 通用的从数仓同步数据到mysql的类
 * 如果存在就是修改记录，如果不存在就添加记录
 */
object hive_to_mysql_insertOrUpdate {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    var dbServer = "192.168.0.201"
    var from_db = "ykchr"
    var from_table = "ads_staff"
    var to_db = "paascloud_mdm"
    var to_table = "staff"
    var userName = "root"
    var passWd = "Mmz@2020"
    var dt = "2020-11-02"
    var dt1 = "2020-11-02"
    val builder = SparkSession.builder()

    builder
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .enableHiveSupport()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      builder.master("local[*]")
    } else {
      dbServer = args(0)
      from_db = args(1)
      from_table = args(2)
      to_db = args(3)
      to_table = args(4)
      userName = args(5)
      passWd = args(6)
      dt = args(7)
      dt1 = args(8)
    }
    builder.appName(from_table)
    val spark = builder.getOrCreate()

    /**
      * 获取目标表（mysql）的结构信息
      */
    val driver = "com.mysql.jdbc.Driver"
    val url = s"jdbc:mysql://$dbServer:3306/$to_db?connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000"
    import scala.collection.JavaConverters._
    val targetTableColumnNameTypes:mutable.Map[String, String] =DatabaseUtil.getColumnNameAndType(dbServer,userName,passWd,to_table).asScala
    val targetTableColumnNameTypesBroadcast=spark.sparkContext.broadcast(targetTableColumnNameTypes)
    var columns = "";
    targetTableColumnNameTypes.foreach(line=>{
      columns = columns+line._1+","
    })
    columns= columns.substring(0,columns.length-1)

    /**
      * 把hive数据保存到mysql
      */
    new IDate {
      override def onDate(dt: String): Unit = {
        val frame = spark.sql(
          s"""
             |select * ,'${DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss")}' as last_update_time from $from_db.$from_table where dt='$dt'
             |""".stripMargin)

        val fields = frame.schema.fields
        frame.rdd.foreachPartition(it=>{
          Class.forName(driver)
          val conn: Connection = DriverManager.getConnection(url, userName, passWd)
          val ps = conn.createStatement
          conn.setAutoCommit(false)
          var batchSize = 0;
          it.foreach(record=>{
            try {
              var sourceTable = new mutable.HashMap[String, String]()
              fields.foreach(StructField => {
                val clunnName = StructField.name.toLowerCase
                val dateType = StructField.dataType
                dateType match {
                  case _: ByteType => sourceTable.put(clunnName, record.getAs[Int](clunnName) + "")
                  case _: ShortType => sourceTable.put(clunnName, record.getAs[Int](clunnName) + "")
                  case _: IntegerType => sourceTable.put(clunnName, record.getAs[Int](clunnName) + "")
                  case _: LongType => sourceTable.put(clunnName, record.getAs[Long](clunnName) + "")
                  case _: BooleanType => sourceTable.put(clunnName, record.getAs[Boolean](clunnName) + "")
                  case _: FloatType => sourceTable.put(clunnName, record.getAs[Float](clunnName) + "")
                  case _: DoubleType => sourceTable.put(clunnName, record.getAs[Double](clunnName) + "")
                  case _: StringType => sourceTable.put(clunnName, record.getAs[String](clunnName) + "")
                  case _: TimestampType => sourceTable.put(clunnName, record.getAs[Timestamp](clunnName) + "")
                  case _: DateType => sourceTable.put(clunnName, record.getAs[Date](clunnName) + "")
                  case _: DecimalType => sourceTable.put(clunnName, record.getAs[Decimal](clunnName) + "")
                  case _ => throw new RuntimeException("nonsupport $ {dateType} !!!")
                }
              })

              var lineValue = new StringBuffer("")
              var updateClumns =  new StringBuffer("")
              val targetTableColumnNameTypesMap = targetTableColumnNameTypesBroadcast.value
              targetTableColumnNameTypesMap.foreach(schecma => {
                val clunnName = schecma._1
                val dateType = schecma._2
                updateClumns.append( "," + clunnName + "=values(" + clunnName + ")")
                dateType match {
                  case "INT" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "ShortType" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "IntegerType" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "LONG" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "BOOLEAN" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "FLOAT" =>lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "DOUBLE" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "DECIMAL" => lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "VARCHAR" => if (StringUtils.isNotBlank(show(sourceTable.get(clunnName))) && !show(sourceTable.get(clunnName)).equals("null")) {
                    lineValue.append(",'").append(show(sourceTable.get(clunnName))).append("'" )
                  } else {
                    lineValue.append(",NULL")
                  }
                  case "TimestampType" =>  lineValue.append(",").append(show(sourceTable.get(clunnName)))
                  case "DATETIME" => if (StringUtils.isNotBlank(show(sourceTable.get(clunnName))) && !show(sourceTable.get(clunnName)).equals("null")) {
                    lineValue.append(",'").append(show(sourceTable.get(clunnName))).append("'")
                  } else {
                    lineValue.append(",NULL")
                  }
                  case _ => throw new RuntimeException("nonsupport $ {dateType} !!!")
                }
              })
              var insertSql = "insert into " + to_table + "(" + columns + ") "
              val lineValueStr = lineValue.toString.substring(1, lineValue.toString.length)
              val updateClumnsStr = updateClumns.toString.substring(1, updateClumns.toString.length)
              insertSql = insertSql + "values(" + lineValueStr + ") on duplicate key update " + updateClumnsStr
              batchSize = batchSize + 1
              if (batchSize < 1000) {
                ps.addBatch(insertSql);
              } else {
                ps.executeBatch()
                conn.commit()
                batchSize = 0
              }
            } catch {
              case  e:Exception =>
                e.getStackTrace()
              /*if(ps!=null){
                ps.close()
              }
              if(conn!=null){
                conn.close()
              }*/
            }
          })
          ps.executeBatch()
          conn.commit()
          /*if(ps!=null){
            ps.close()
          }
          if(conn!=null){
            conn.close()
          }*/
        })
      }
    }.invoke(dt, dt1)

    spark.stop()
  }

  def show(x: Option[String]) = x match {
    case Some(s) => s
    case None => "null"
  }

}
