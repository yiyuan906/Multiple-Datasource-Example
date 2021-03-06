package com.yiyuan.FinalOne.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class datasourceARelation extends readType {
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

  def methodOfReading(): String = {
    "Line"
  }

  override def wayOfReading(): String = "text"
}
