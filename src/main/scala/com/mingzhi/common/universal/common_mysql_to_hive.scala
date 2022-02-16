package com.mingzhi.common.universal

import com.mingzhi.ConfigUtils
import com.mingzhi.common.interf.{IDate, LoadStrategy, MySaveMode}
import com.mingzhi.common.utils.{SinkUtil, SourceUtil, SparkUtils, StringUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * parquet不支持date日期格式的问题：
 * UnsupportedOperationException Parquet does not support date. See HIVE-6384
 * 因此新增该类，增加hive格式参数parquet or orc json
 * 增加mysql的port参数支持
 * 不支持自定义hive表名，也就是说hive表名和源mysql表名原则上一致，但是支持前缀以区分不同业务库的同名表
 * 并统一导入策略
 * 支持全量导数策略下的多表批量导入
 */
object common_mysql_to_hive {

  //===========================params define=====================================
  /**
   * ip or "ip:port"
   */
  private var mysql_host: String = ""
  private var mysql_db = ""
  private var mysql_tables = ""
  private var mysql_username = ""
  private var mysql_pwd = ""


  private var hive_db = ""
  private var hive_table_prefix = "" //usually is empty
  private var format = "parquet"

  private var dt: String = ""
  private var dt1: String = ""

  /**
   * "ct_or_ut_time" or ""
   */
  private var create_time = ""
  private var update_time = ""

  /**
   * 增加该参数的目的：
   * 避免业务库频繁的更改表名直接对数仓表名的影响
   */
  private var hive_tables = "";
  //================================================================

  private var mysql_port = "3306"


  /**
   * 默认抽数策略为抽新增
   * 实际根据程序传参参数确定策略
   */
  var load_strategy: LoadStrategy.Value = LoadStrategy.NEW

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val builder = SparkUtils.getBuilder

    //本地或者集群
    if (System.getProperties.getProperty("os.name").contains("Windows")) {

      builder.master("local[*]")


      mysql_host = s"${ConfigUtils.SEGI_MYSQL_HOST}:3308"
      //      mysql_host = s"${ConfigUtils.UAC_MYSQL_HOST}:3306"
      mysql_db = "vwork"
      //      mysql_db = "paascloud_uac_203"
      //      mysql_table = "si_main,hr_staff_base,hr_staff_base_ext,hr_staff_certificate_management,hr_staff_edu,hr_staff_contract" +
      //        ",pt_post_genre_sp_mingzhe,pt_code_type_value,pt_post_level" +
      //        ",pf_cost_seal,si_cost_seal" +
      //        ",pt_staff_stru"

      mysql_tables = "si_main"

      //      mysql_table = "ctt_contract,ctt_contract_attr_inst,ctt_target,ctt_contract_user_info,segi_organ,segi_tb_uhome_region,ctt_template_attr_value"
      //      mysql_table = "t_uac_organization,t_uac_building,t_uac_building_room,t_uac_organization_auth,t_uac_organization_expand,t_uac_subsystem,t_uac_user"
      mysql_username = ConfigUtils.SEGI_MYSQL_USERNAME
      mysql_pwd = ConfigUtils.SEGI_MYSQL_PWD


      hive_db = "hr"
      //      hive_db = "contract"
      //      hive_db = "paascloud"
      hive_table_prefix = ""
      format = "orc"

      /**
       * 全量：“”,""
       * 增量之新增：create_time,""
       * 增量之新增和变化：crate_time,update_time
       */
      create_time = ""
      update_time = ""

      dt = "2022-01-01"
      dt1 = "2022-01-01"

      hive_tables = "si_main_ext2"


    } else {
      mysql_host = args(0)
      mysql_db = args(1)
      mysql_tables = args(2)
      mysql_username = args(3)
      mysql_pwd = args(4)
      hive_db = args(5)
      hive_table_prefix = args(6)
      format = if (StringUtils.isEmpty(args(7))) "parquet" else args(7)

      create_time = args(8)
      update_time = args(9)

      dt = args(10)
      dt1 = args(11)

      if (args.length == 13) {
        hive_tables = args(12)
      }
    }

    StringUtil.assertNotBlank(mysql_host, "mysql_host can not be null")
    StringUtil.assertNotBlank(mysql_db, "mysql_db can not be null")
    StringUtil.assertNotBlank(mysql_tables, "mysql_tables can not be null")
    StringUtil.assertNotBlank(mysql_username, "mysql_username can not be null")
    StringUtil.assertNotBlank(mysql_pwd, "mysql_pwd can not be null")
    StringUtil.assertNotBlank(hive_db, "hive_db can not be null")

    if (dt.length != "yyyy-MM-dd".length || dt1.length != "yyyy-MM-dd".length) {
      throw new IllegalArgumentException("dt格式必须为yyyy-MM-dd")
    }


    /**
     * 参数解析，确定抽数策略
     */
    if (mysql_host.split(":").length >= 2) {
      mysql_port = mysql_host.split(":")(1)
      mysql_host = mysql_host.split(":")(0)
    }

    if (StringUtils.isBlank(create_time) && StringUtils.isBlank(update_time)) {

      load_strategy = LoadStrategy.ALL
      println("1 load_strategy:" + load_strategy)

    } else if (StringUtils.isNotBlank(create_time) && StringUtils.isBlank(update_time)) {

      load_strategy = LoadStrategy.NEW
      println("2 load_strategy:" + load_strategy)

    } else if (StringUtils.isNotBlank(create_time) && StringUtils.isNotBlank(update_time)) {

      load_strategy = LoadStrategy.NEW_AND_CHANGE
      println("3 load_strategy:" + load_strategy)
    } else {
      throw new IllegalArgumentException("create_time 和 update_time参数组合异常")
    }

    val spark: SparkSession = builder
      .appName("mysql_to_hive")
      .getOrCreate()

    println("load_strategy:" + load_strategy)

    var tables: Array[(String, String)] = mysql_tables.split(",").zip(mysql_tables.split(","))

    if (StringUtils.isNotBlank(hive_tables)) {

      tables = mysql_tables.split(",").zip(hive_tables.split(","))

    }


    if (tables.length > 1 && load_strategy != LoadStrategy.ALL) {
      throw new IllegalArgumentException("非全量导入不支持多表批量操作...")
    }

    new IDate {
      override def onDate(dt: String): Unit = {

        tables
          .foreach(t => {

            val frame_result = process(spark, dt, t._1, load_strategy)

            println(s"frame_result of ${t._1}===>")
            frame_result.show(false)

            /**
             * 全量表不进行分区，全表覆盖，只有一份
             */
            val mySaveMode = if (load_strategy == LoadStrategy.ALL) MySaveMode.OverWriteAllTable else MySaveMode.OverWriteByDt

            val t_hive = if (StringUtils.isNotBlank(t._2)) t._2 else t._1

            SinkUtil.sink_to_hive(dt, spark, frame_result, hive_db, s"$hive_table_prefix$t_hive", format, mySaveMode)

          })

      }
    }.invoke(dt, dt1)

    spark.stop()

  }

  def process(spark: SparkSession, dt: String, t_source: String, load_strategy: LoadStrategy.strategy): DataFrame = {

    var frame_mysql: DataFrame = null
    var frame_result: DataFrame = null

    println("process load_strategy:" + load_strategy)

    if (load_strategy == LoadStrategy.NEW) {

      frame_result = SourceUtil.sourceNewFromMysql(spark, mysql_host, mysql_port, mysql_db, t_source, mysql_username, mysql_pwd, create_time, dt)
        .withColumn("dt", lit(dt))

    } else if (load_strategy == LoadStrategy.ALL) {

      frame_mysql = SourceUtil.sourceAllFromMysql(spark, mysql_host, mysql_port, mysql_db, t_source, mysql_username, mysql_pwd)

      frame_result = frame_mysql.withColumn("dt", lit(dt))

    } else if (load_strategy == LoadStrategy.NEW_AND_CHANGE) {

      frame_result = SourceUtil.sourceNewAndChangeFromMysql(spark, mysql_host, mysql_port, mysql_db, t_source, mysql_username, mysql_pwd, create_time, update_time, dt)
        .withColumn("dt", lit(dt))
    }

    frame_result
  }
}
