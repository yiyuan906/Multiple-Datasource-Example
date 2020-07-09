package com.yiyuan.workingClass

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class datasourceTestTRelation {
  def relationReturn(relationContext:SQLContext, relationPath:String):BaseRelation = {
    new BaseRelation with PrunedScan {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("ID",IntegerType))
        .add(StructField("FirstName",StringType))

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        relationContext.sparkContext.textFile(relationPath).map{x=>
          val split = x.split(",")

          val values = requiredColumns.map{
            case "ID" => split(2).toInt
            case "FirstName" => split(1)
          }
          Row.fromSeq(values)
        }
      }
    }
  }
  def dfReturn(relationContext:SQLContext, relationPath:String):DataFrame = {
    import relationContext.implicits._
    relationContext.sparkContext.textFile(relationPath).map{x=>
      val split = x.split(",")
     (split(2).toInt,split(1))
    }.toDF()
  }
}
