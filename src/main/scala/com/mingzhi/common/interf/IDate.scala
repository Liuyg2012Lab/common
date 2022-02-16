package com.mingzhi.common.interf

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

trait IDate extends Serializable {

  def onDate(dt: String): Unit


  def invoke(dt: String, dt1: String) = {

    if (dt1 < dt) {
      throw new IllegalArgumentException(s"dt1:${dt1}小于dt:${dt}")
    }


    // 日期格式化
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    // 起始日期
    val d1: Date = sdf.parse(dt)
    // 结束日期
    var d2: Date = sdf.parse(dt1)
    var tmp: Date = d1
    var dd: Calendar = Calendar.getInstance
    dd.setTime(d1)


    while (tmp.getTime <= d2.getTime) {
      tmp = dd.getTime
      System.out.println("IDate遍历日期:" + sdf.format(tmp))


      onDate(sdf.format(tmp))

      // 天数加上1
      dd.add(Calendar.DAY_OF_MONTH, 1)
      tmp = dd.getTime
    }

  }
}
