package com.mingzhi.common.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

object SourceUtil {

  def sourceFromEsByDt(es_host: String, es_index: String, dt: String, appName: String): (SparkSession, DataFrame) = {

    val spark: SparkSession = getSpark(es_host, appName)

    val esQuery =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": {
         |        "range": {
         |          "dt": {
         |            "gte": "$dt",
         |            "lte": "$dt"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
         |""".stripMargin

    (spark, EsSparkSQL.esDF(spark, s"/${es_index}/_doc", esQuery))
  }

  private def getSpark(es_host: String, appName: String): SparkSession = {
    val builder = SparkUtils.getBuilder

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")

    }

    val spark: SparkSession = builder
      .config("es.nodes", es_host)
      .appName(appName)
      .getOrCreate()

    spark

  }

  def sourceAllFromMysql(spark: SparkSession, host: String, db: String, t_source: String, user_name: String, pwd: String): DataFrame = {

    sourceAllFromMysql(spark, host, "3306", db, t_source, user_name, pwd)
  }

  /**
   * 单并行度读mysql
   */
  def sourceAllFromMysql(spark: SparkSession, host: String, port: String, db: String, t_source: String, user_name: String, pwd: String): DataFrame = {


    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://${host}:${port}/${db}?tinyInt1isBit=false&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000")
      .option("user", user_name)
      .option("password", pwd)
      .option("dbtable", t_source)
      .load()

    jdbcDF.repartition(3)

  }


  def sourceNewFromMysql(spark: SparkSession, host: String, port: String, db: String, t_source: String, user_name: String, pwd: String, createTime: String, dt: String): DataFrame = {

    val url = s"jdbc:mysql://${host}:${port}/${db}?tinyInt1isBit=false&user=${user_name}&password=${pwd}&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000"

    val expression = s"date_format($createTime,'%Y-%m-%d') = '$dt'"

    val jdbcDF: DataFrame = spark.read.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> s"(SELECT * FROM ${t_source} where $expression) t"))
      .load()

    jdbcDF.repartition(3)

  }

  def sourceNewAndChangeFromMysql(spark: SparkSession, host: String, port: String, db: String, t_source: String, user_name: String, pwd: String, createTime: String, updateTime: String, dt: String): DataFrame = {

    val url = s"jdbc:mysql://${host}:${port}/${db}?tinyInt1isBit=false&user=${user_name}&password=${pwd}&connectionRequestTimout=300000&connectionTimeout=300000&socketTimeout=300000"

    val expression = s"date_format($createTime,'%Y-%m-%d') = '$dt' or date_format($updateTime,'%Y-%m-%d') = '$dt'"

    println("expression:" + expression)

    val jdbcDF: DataFrame = spark.read.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> s"(SELECT * FROM ${t_source} where $expression) t"))
      .load()

    jdbcDF.repartition(3)

  }


  def sourceAllFromEs(spark: SparkSession, index: String, _doc: String): DataFrame = {
    val esQuery =
      s"""
         |{"query":{"match_all":{}}}
       """.stripMargin
    val resDF: DataFrame = EsSparkSQL.esDF(spark, s"/${index}/${_doc}", esQuery)
    resDF.repartition(3)
  }


  def sourceNewFromEs(spark: SparkSession, index: String, _doc: String, createTime: String, dt: String): DataFrame = {

    val esQuery =
      s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": {
         |        "range": {
         |          "${createTime}": {
         |            "gte": "${dt} 00:00:00",
         |            "lte": "${dt} 23:59:59"
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
         |""".stripMargin

    EsSparkSQL.esDF(spark, s"/${index}/${_doc}", esQuery)
      .repartition(3)
  }

  def sourceNewAndChangeFromEs(spark: SparkSession, index: String, _doc: String, createTime: String, updateTime: String, dt: String): DataFrame = {

    val esQuery =
      s"""
         |{
         |  "query":
         |  {
         |    "bool":
         |    {
         |      "should":
         |      [
         |        {
         |        "range": {
         |          "$createTime":
         |          {
         |            "gte": "$dt 00:00:00",
         |            "lte": "$dt 23:59:59"
         |          }
         |        }
         |      },
         |      {
         |        "range": {
         |          "$updateTime":
         |          {
         |            "gte": "$dt 00:00:00",
         |            "lte": "$dt 23:59:59"
         |          }
         |        }
         |      }
         |    ]
         |    }
         |  }
         |}
         |""".stripMargin

    EsSparkSQL.esDF(spark, s"/${index}/${_doc}", esQuery)
      .repartition(3)
  }
}
