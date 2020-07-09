package com.yiyuan.workingClass

import java.sql.Date

import org.apache.spark.sql.{DataFrame, SQLContext}

class datasourceARelation {
  def dfReturn(relationContext:SQLContext, relationPath:String):DataFrame = {
    import relationContext.implicits._
    relationContext.sparkContext.textFile(relationPath).map{x=>
      val split = x.split(" ")
      val desc = x.drop((split(0)+split(1)+split(2)+split(3)+split(4)+split(5)).size+6)
      (Date.valueOf(split(0)),split(1),split(3),split(2),split(5).toInt,desc)
    }.toDF()
  }
}
