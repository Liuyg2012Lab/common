package com.mingzhi.common.test

import com.mingzhi.common.accumulator.WordCountAccumulator
import com.mingzhi.common.utils.{SparkUtils, TableUtils}
import org.apache.spark.HashPartitioner
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{lit, var_pop}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable

object test1 {

  val dt: String = "2021-04-01"
  val dt1: String = "2021-04-10"

  private val spark: SparkSession = SparkUtils.getBuilder.master("local[1]").getOrCreate()

  def main(args: Array[String]): Unit = {

    //    testDiff()

    //    testUnion()

    //        testChangedData()

    //    testMapAccumulator()

    //    testSplit()


    testZip()

    //    testFormat()

    //    testMapCombine()

//    testForeach()
  }

  def testForeach(): Unit = {

    "".split(",").foreach(x => {
      println("x:" + x)
    })

  }

  def testMapCombine(): Unit = {

    var map1 = mutable.Map[String, String]()
    val map2 = mutable.Map[String, String]()

    map1.put("a", "1")
    map2.put("b", "2")

    map1 = map1 ++ map2

    println("result:" + map1)
  }

  def testFormat(): Unit = {

    import spark.implicits._
    val frame = spark.sparkContext.makeRDD(Array(10, 20, 30, 40, 50, 60)).toDF("age")

    frame.show(false)

    val rdd1: RDD[(Row, Int)] = frame.rdd.map(x => {
      (x, 1)
    })

    rdd1.partitionBy(new HashPartitioner(10))
      .partitionBy(new HashPartitioner(10))


    frame
      .coalesce(1)
      .write
      .format("orc") //orc parquet json csv text
      .mode(SaveMode.Overwrite)
      .saveAsTable("iot.test")
  }

  case class A(age: Int)

  def testZip(): Unit = {

    val a = "a,b,c"
    val b = a

    a.split(",").zip(b.split(","))
      .foreach(x => {
        println(x._1 + "=>" + x._2)
      })

  }

  def testSplit(): Unit = {

    //    var index = "xxx_yy_z_index_2021"
    var index = "xxx_yy_z_index_2021:_docx"

    val post_fix = "_"

    if (index.split(":").length == 1) {

      index = index.substring(0, index.lastIndexOf(post_fix))

      println(index)

    } else if (index.split(":").length == 2) {

      val pos = index.split(":")(0).lastIndexOf(post_fix)

      index = index.split(":")(0).substring(0, pos) + ":" + index.split(":")(1)

      println(index)
    }


  }

  def testMapAccumulator(): Unit = {

    val accumulator = new WordCountAccumulator()

    val context = spark.sparkContext

    context.register(acc = accumulator, "WordCountAcc")

    //    val words: RDD[String] = context.parallelize(List("a", "b", "c", "a", "b", "d"))
    val words: RDD[String] = context.parallelize(List("a"))

    println("partitionsNum:" + words.getNumPartitions)

    words.foreach(w => {
      //      println(w)

      accumulator.add(w)
    })


    //    val value: mutable.Map[String, Long] = accumulator.value
    //
    //    println("value:" + value)

  }


//  def testChangedData(): Unit = {
//    val dates: Array[String] = TableUtils.getExecuteDate(spark, "paascloud.ods_order", "2021-09-06")
//    dates.foreach(dt => {
//      println("changed dt:" + dt)
//    })
//  }

  def testDiff(): Unit = {

    var frame: DataFrame = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L, "20210411"),
      ("ming", 20, 15552211521L, "20210412"),
      ("lucy", 20, 15552211521L, "20210413"),
      ("hong", 19, 13287994007L, "2021-04-12 00:00:05"),
      ("zhi", 21, 15552211523L, "2021-04-12")
    )).toDF("name", "age", "phone", "Month")


    var diff_minus: Set[String] = Set()
    var diff_plus: Set[String] = Set()

    var s1 = Set("a", "age")
    var s2 = Set("a", "c", "d")

    diff_minus = s1 diff (s2)
    diff_plus = s2 diff (s1)

    for (elem <- diff_minus) {
      println("drop col " + elem)
      frame = frame.drop(elem)
    }

    for (elem <- diff_plus) {
      println("add col " + elem)
      frame = frame.withColumn(elem, lit(""))
    }

    frame.show(false)
  }


  def testUnion() = {

    val df1 = spark.createDataFrame(Seq((1, 2, 3))).toDF("col0", "col1", "col2")
    val df2 = spark.createDataFrame(Seq((4, 5, 6))).toDF("col1", "col2", "col0")

    /**
     * +----+----+----+
     * |col0|col1|col2|
     * +----+----+----+
     * |   1|   2|   3|
     * |   4|   5|   6|
     * +----+----+----+
     */
    df1.union(df2).show

  }
}
