20210223

业务数据sink到hive表,由原来新增字段的删除改为同步添加
   spark.sql(
     s"""
        |ALTER TABLE ${hive_db}.${hive_table} ADD COLUMNS(${x} STRING)
        |""".stripMargin)
