package com.mingzhi.common.interf

import org.apache.spark.annotation.InterfaceStability

@InterfaceStability.Stable
object MySaveMode extends Enumeration {
  type MySaveMode = Value
  val

  /**
   * 全表覆盖
   */
  OverWriteAllTable,

  /**
   * 按dt进行sink，覆盖
   */
  OverWriteByDt,

  /**
   * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
   * the save operation is expected to not save the contents of the DataFrame and to not
   * change the existing data.
   *
   * @since 1.3.0
   */
  Ignore = Value
}
