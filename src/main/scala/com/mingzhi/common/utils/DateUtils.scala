package com.mingzhi.common.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

object DateUtils {

  def main(args: Array[String]): Unit = {

    val timestamp = getMillsFromString("2020-12-28 11:59:33")
    val windowSize = 24 * 60 * 60 * 1000l
    val offset = -8 * 60 * 60 * 1000l
    val w_start: Long = getWindowStartWithOffset(timestamp, offset, windowSize)
    println("w_start:" + getDateStrFromMill(w_start))

  }

  def getWindowStartWithOffset(timestamp: Long, offset: Long, windowSize: Long): Long = {
    timestamp - (timestamp - offset + windowSize) % windowSize
  }

  def getMillsFromString(str: String): Long = {
    var millionSeconds = 0L
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      millionSeconds = sdf.parse(str).getTime //毫秒5261

    } catch {
      case e: Exception => e.printStackTrace()
    }

    millionSeconds
  }

  def getDateStrFromMill(mills: Long): String = {

    val date = new Date(mills)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    sdf.format(date)

  }

  def back1Quarter(t: String): String = {
    getDateStrFromMill(getMillsFromString(t) - 15 * 60 * 1000)

  }

  def next1Quarter(t: String): String = {
    getDateStrFromMill(getMillsFromString(t) + 15 * 60 * 1000)

  }

  def back1Day(t: String): String = {
    getDateStrFromMill(getMillsFromString(t) - 60 * 60 * 1000 * 24)

  }

  def back1Hour(t: String): String = {
    getDateStrFromMill(getMillsFromString(t) - 60 * 60 * 1000)

  }

  def back1Week(t: String): String = {
    getDateStrFromMill(getMillsFromString(t) - 60 * 60 * 1000 * 168)

  }

  import java.text.SimpleDateFormat


  def isValidDate(str: String) = {
    var valid = true
    // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      // 设置lenient为false.
      // 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
      format.setLenient(false)
      format.parse(str)
    } catch {
      case e: Exception =>
        // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
        valid = false
    }
    valid
  }

  @throws[Exception]
  def getWeekFromDay(pTime: String): Int = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val c = Calendar.getInstance
    c.setTime(format.parse(pTime))
    var dayForWeek = 0
    if (c.get(Calendar.DAY_OF_WEEK) == 1) {
      dayForWeek = 7
    }
    else dayForWeek = c.get(Calendar.DAY_OF_WEEK) - 1
    dayForWeek
  }

}
