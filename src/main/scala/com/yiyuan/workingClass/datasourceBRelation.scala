package com.yiyuan.workingClass

import java.sql.Date

import org.apache.spark.sql.{DataFrame, SQLContext}

class datasourceBRelation {
  def dfReturn(relationContext:SQLContext, relationPath:String):DataFrame = {
    import relationContext.implicits._
    relationContext.read.option("header",true).option("delimiter",",")
      .csv(relationPath)
      .map(_.mkString(" ")).rdd
      .map{ line => {
        val split = line.split(" ")
        val desc = line.drop((split(0)+split(1)+split(2)+split(3)+split(4)+split(5)+split(6)+split(7)).size+8)
        (Date.valueOf(split(1)),split(4),split(7),split(2),split(5).toInt,desc)
        }
      }.toDF()
  }
}
