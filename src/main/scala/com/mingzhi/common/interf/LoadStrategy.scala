package com.mingzhi.common.interf

import org.apache.spark.annotation.InterfaceStability

@InterfaceStability.Stable
object LoadStrategy extends Enumeration {
  type strategy = Value
  val

  ALL,

  NEW_AND_CHANGE,

  NEW = Value
}
