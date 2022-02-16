package com.mingzhi.common.test

import org.apache.spark.sql.{SaveMode, SparkSession}

object ClickHouseTest {
  def main(args: Array[String]): Unit = {

    //    val table=args(0)
    //    val ckTableN=table.split("\\.")(1)

    val table = "stg.im_md_log_pro"
    val ckTableN = "ept.im_md_log_pro"

    //    print(ckTableN)

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("clickhouse test")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql(s"select * from $table")

    df.show(false)

    val ckDriver = "ru.yandex.clickhouse.ClickHouseDriver"
    val ckUrl = "jdbc:clickhouse://10.248.31.22:8123/ept"
    //    val ckTableN ="im_md_log_pro_action"
    var pro = new java.util.Properties
    pro.put("driver", ckDriver)
    //    默认写入批次是2w，可以调大至5w
    df.write.mode(SaveMode.Append).option("batchsize", "50000")
      .option("isolationLevel", "NONE")
      .option("numPartitions", "50").jdbc(ckUrl, ckTableN, pro)

    spark.close()

  }
}
