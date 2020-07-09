package com.yiyuan.workingClass

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class datasourceUserdataRelation {
  def relationReturn(relationContext:SQLContext, relationPath:String):BaseRelation = {
    new BaseRelation with PrunedScan {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("ID", IntegerType,true))
        .add(StructField("FirstName", StringType,true))

      import relationContext.implicits._

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        relationContext.read.option("header",true).option("delimiter",",")
          .csv(relationPath)
          .map(_.mkString(",")).rdd
          .map{x=>
            val split = x.split(",")

            val values = requiredColumns.map{
              case "ID" => split(1).toInt
              case "FirstName" => split(2)
            }
            Row.fromSeq(values)
          }
      }
    }
  }
  def dfReturn(relationContext:SQLContext, relationPath:String):DataFrame = {
    import relationContext.implicits._

    relationContext.read.option("header",true).option("delimiter",",")
      .csv(relationPath)
      .map(_.mkString(",")).rdd
      .map{x=>
        val split = x.split(",")
        (split(1).toInt, split(2))
      }.toDF()
  }
}
