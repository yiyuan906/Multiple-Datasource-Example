package com.yiyuan.workingClass

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SQLContext}
import java.sql.Timestamp
import java.text.SimpleDateFormat

class datasourceSwimRelation {
  def relationReturn(relationContext:SQLContext, relationPath:String):BaseRelation = {
    new BaseRelation with PrunedScan with java.io.Serializable {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("activity_type",StringType,true))
        .add(StructField("Timestamp",TimestampType,true))
        .add(StructField("Favorite",BooleanType,true))
        .add(StructField("Title",StringType,true))
        .add(StructField("Distance",IntegerType,true))              //5
        .add(StructField("Calories",StringType,true))
        .add(StructField("Time",StringType,true))
        .add(StructField("Avg_pace",StringType,true))
        .add(StructField("Best_pace", StringType,true))
        .add(StructField("Flow",StringType,true))                   //10
        .add(StructField("total_strokes", IntegerType, true))
        .add(StructField("avg_swolf", IntegerType, true))
        .add(StructField("avg_stroke_rate", DoubleType,true))
        .add(StructField("min_temp",DoubleType,true))
        .add(StructField("Decompression",StringType,true))           //15
        .add(StructField("Best_lap_time",StringType,true))
        .add(StructField("Num_of_runs",IntegerType))

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        import relationContext.implicits._
        val split = relationContext.read.option("header",true).option("delimiter",";")
          .csv("swimming.csv")
          .map(x=>x.toString()).rdd
          .map{ line=>{
            val split = line.split(",")

            val values = requiredColumns.map {
              case "activity_type" => split(0).drop(1)
              case "Timestamp" => {
                val format = new SimpleDateFormat("dd/MM/yyyy HH:mm")
                new Timestamp(format.parse(split(1)).getTime)
              }
              case "Favorite" => split(2).toBoolean
              case "Title" => split(3)
              case "Distance" => (split(4)+split(5)).toInt
              case "Calories" => split(6)
              case "Time" => split(7)
              case "Avg_pace" => split(8)
              case "Best_pace" => split(9)
              case "Flow" => split(10)
              case "total_strokes" => split(11).toInt
              case "avg_swolf" => split(12).toInt
              case "avg_stroke_rate" => split(13).toDouble
              case "min_temp" => split(14).toDouble
              case "Decompression" => split(15)
              case "Best_lap_time" => split(16)
              case "Num_of_runs" => split(17).dropRight(1).toInt
            }
            Row.fromSeq(values)
          }}
        split
      }
    }
  }
}
