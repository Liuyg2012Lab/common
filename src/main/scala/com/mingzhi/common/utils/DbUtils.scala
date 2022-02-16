package com.mingzhi.common.utils

import java.sql.{Connection, Statement}

object DbUtils {

  /**
   * 释放资源
   */
  def release(connection: Connection, statement: Statement): Unit = {
    println("release Connection===========")

    try {
      if (statement != null) {
        statement.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
