package com.yiyuan.FinalOne.dataloader

import org.apache.spark.sql.Row

trait textParsing {
  def lineParsing(line: String): Row
}
