package com.yiyuan.FinalOne.dataloader

import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.spark.sql.Row

trait parquetParsing {
  def groupParsing(grp:ParquetReader[Group]): Row
}
