package com.yiyuan.workingClass

import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class datasourceBRelation {
  def dfReturn(relationContext:SQLContext, relationPath:String):RDD[Row] = {
    import relationContext.implicits._
    relationContext.read.option("header",true).option("delimiter",",")
      .csv(relationPath)
      .map(_.mkString(" ")).rdd
      .map{ line => {
        val split = line.split(" ")
        val desc = line.drop((split(0)+split(1)+split(2)+split(3)+split(4)+split(5)+split(6)+split(7)).length+8)
        Row(split(1),split(4),split(7),split(2),split(5).toInt,desc) //Date.valueOf(split(1))
        }
      }
  }

  def streamParsing(stringStream:String): Row = {
    val split = stringStream.split(",")
    val desc = stringStream.drop((split(0)+split(1)+split(2)+split(3)+split(4)+split(5)+split(6)+split(7)).length+8)
    Row(split(1),split(4),split(7),split(2),split(5).toInt,desc) //Date.valueOf(split(1))
  }
}
