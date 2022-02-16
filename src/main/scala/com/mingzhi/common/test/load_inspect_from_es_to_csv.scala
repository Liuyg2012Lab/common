package com.mingzhi.common.test

import com.mingzhi.common.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL

object load_inspect_from_es_to_csv {

  def main(args: Array[String]): Unit = {

    val conf = SparkUtils.getDefConf

    var csvPath = "hdfs://mz-hadoop-01:8020/ck/inspect"

    val builder = SparkSession.builder()

    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      conf.host = "192.168.20.145"
      conf.from_db = "ins_ordermain_report"
      conf.from_table = "insordermainreport_doc"
      conf.dt = "2020-12-26"
      conf.dt1 = "2020-12-26"
      builder.master("local[*]")

    } else {
      conf.host = "172.18.253.56"
      conf.from_db = "ins_ordermain_report"
      conf.from_table = "insordermainreport_doc"
      conf.dt = args(0)
      conf.dt1 = args(1)
      csvPath = args(2)
    }
    builder
      .config("es.nodes", conf.host)
      .config("es.port", "9200")
      .config("es.mapping.date.rich", value = false)
      .config("dfs.replication", 1)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config(ConfigurationOptions.ES_SCROLL_SIZE, 10000)
      .config(ConfigurationOptions.ES_MAX_DOCS_PER_PARTITION, 1000000)
      .config(ConfigurationOptions.ES_HTTP_TIMEOUT, "5m")
      .config(ConfigurationOptions.ES_SCROLL_KEEPALIVE, "10m")
      .config("spark.debug.maxToStringFields", "200")
      .appName(conf.from_table)
      .enableHiveSupport()
    val spark: SparkSession = builder.getOrCreate()


    //    new IDate {
    //      override def onDate(dt: String): Unit = {
    //
    //
    //
    //      }
    //    }.invoke(conf.dt, conf.dt1)


    val esQuery =
      s"""
         |
         |{
         |    "query" : {
         |    "bool" : {
         |      "must" : [
         |        {
         |          "range" : {
         |            "createTime" : {
         |              "from" : "${conf.dt} 00:00:00",
         |              "to" : "${conf.dt1} 23:59:59"
         |            }
         |          }
         |        }
         |      ]
         |    }
         |  }
         |}
         |
         |""".stripMargin

    val f1: DataFrame = EsSparkSQL.esDF(spark, s"/${conf.from_db}/${conf.from_table}", esQuery)

      /**
       * "id"
       * "orderId"
       * "orderName"
       * "proposeUserName"
       * "organizationId"
       * "lineName"
       * "descs"
       * "planStartTime"
       * "planEndTime"
       * "startTime"
       * "endTime"
       * organizationName
       */
      .select("id"
        , "orderId"
        , "orderName"
        //                , "proposeUserName"
        , "organizationId"
        , "lineName"
        , "descs"
        , "planStartTime"
        , "planEndTime"
        , "startTime"
        , "endTime"
        //                , "organizationName"
      )

    f1.show(false)


    //    var inputStream: InputStream = getClass.getResourceAsStream("/institutions")
    //    var somethingFile: File = File.createTempFile("test", ".txt");
    //
    //    try {
    //      FileUtils.copyInputStreamToFile(inputStream, somethingFile);
    //    } finally {
    //      IOUtils.closeQuietly(inputStream);
    //    }

    //    println("file path:" + somethingFile.getAbsolutePath)

    val arr = Array("1330",
      "1337",
      "1345",
      "1346",
      "1507",
      "1759",
      "1760",
      "1761",
      "1763",
      "1790",
      "1791",
      "1792",
      "1793",
      "1794",
      "1795",
      "1796",
      "1797",
      "1798",
      "1799",
      "1800",
      "1801",
      "1802",
      "1803",
      "1804",
      "1805",
      "1806",
      "1807",
      "1808",
      "1809",
      "1810",
      "1811",
      "1812",
      "1813",
      "1814",
      "1815",
      "1816",
      "1817",
      "1818",
      "1819",
      "2326",
      "2333",
      "2338",
      "2343",
      "2353",
      "2358",
      "2365",
      "2372",
      "2373",
      "2390",
      "2439",
      "2440",
      "2624",
      "2747",
      "2804",
      "2834",
      "2835",
      "2836",
      "2837",
      "2838",
      "2852",
      "480733549338902528",
      "486512995403636736",
      "490878791512784896",
      "506467641529139200",
      "512952621457383424",
      "51628603051180032011337274358358")

    import spark.implicits._

    val f_institutions = spark.sparkContext.makeRDD(arr).toDF().withColumnRenamed("value", "institution")


    //    val f_institutions: DataFrame = spark.read.textFile("file:///"+somethingFile.getAbsolutePath).withColumnRenamed("value", "institution")

    f_institutions.show(false)


    val f_joined = f1.join(f_institutions, f1.col("organizationId") === f_institutions.col("institution"), "inner")
      .drop("institution")

    f_joined.show(false)

    f_joined.printSchema()

    //    f_joined
    //      .coalesce(1)
    //      .write
    //      .mode(SaveMode.Overwrite)
    //      .option("header", "true")
    //      .csv(path = csvPath)

    f_joined
      .coalesce(1)
      .write
      //      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"paascloud.x")

    spark.stop()
  }
}
