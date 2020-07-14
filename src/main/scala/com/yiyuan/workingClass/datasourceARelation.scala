package com.yiyuan.workingClass

import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class datasourceARelation {
  def dfReturn(relationContext: SQLContext, relationPath: String): RDD[Row] = {
    relationContext.sparkContext.textFile(relationPath).map { x =>
      val split = x.split(" ")
      val desc = x.drop((split(0) + split(1) + split(2) + split(3) + split(4) + split(5)).length + 6)
      Row(split(0),split(1), split(3), split(2), split(5).toInt, desc) //Date.valueOf(split(0))
     }
  }

  def streamParsing(stringStream:String): Row = {
    val split = stringStream.split(" ")
    val desc = stringStream.drop((split(0) + split(1) + split(2) + split(3) + split(4) + split(5)).length + 6)
    Row(split(0),split(1), split(3), split(2), split(5).toInt, desc)
  }
}
