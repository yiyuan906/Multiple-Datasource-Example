package com.yiyuan.workingClass

import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

class datasourceISSRelation {
  def relationReturn(relationContext:SQLContext, relationPath:String):BaseRelation = {
    new BaseRelation with PrunedScan {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("Date",DateType,true))
        .add(StructField("Time",StringType, true))
        .add(StructField("cs-method", StringType, true))
        .add(StructField("cs-uri-stem", StringType, true))
        .add(StructField("cs-uri-query", StringType, true))
        .add(StructField("s-port", IntegerType, true))      //int
        .add(StructField("cs-username", StringType, true))
        .add(StructField("c-ip", StringType,true))
        .add(StructField("cs-version",StringType,true))
        .add(StructField("cs(User-Agent)",StringType,true))
        .add(StructField("cs(Cookie)",StringType,true))
        .add(StructField("cs(Referer)",StringType,true))
        .add(StructField("cs-host",StringType,true))
        .add(StructField("sc-status",IntegerType,true))    //int
        .add(StructField("sc-substatus",IntegerType,true)) //int
        .add(StructField("sc-win32-status",IntegerType,true)) //int
        .add(StructField("sc-bytes", IntegerType,true)) //int
        .add(StructField("cs-bytes", IntegerType,true)) //int
        .add(StructField("Time-taken", IntegerType,true)) //int

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        val split = relationContext.sparkContext.textFile(relationPath).map(line =>{
          val Elem = line.split(" ")

          val values = requiredColumns.map{
            case "Date" => Date.valueOf(Elem(0))
            case "Time" => Elem(1)
            case "cs-method" => Elem(2)
            case "cs-uri-stem" => Elem(3)
            case "cs-uri-query" => Elem(4)
            case "s-port" => Elem(5).toInt
            case "cs-username" => Elem(6)
            case "c-ip" => Elem(7)
            case "cs-version" => Elem(8)
            case "cs(User-Agent)" => Elem(9)
            case "cs(Cookie)" => Elem(10)
            case "cs(Referer)" => Elem(11)
            case "cs-host" => Elem(12)
            case "sc-status" => Elem(13).toInt
            case "sc-substatus" => Elem(14).toInt
            case "sc-win32-status" => Elem(15).toInt
            case "sc-bytes" => Elem(16).toInt
            case "cs-bytes" => Elem(17).toInt
            case "Time-taken" => Elem(18).toInt
          }
          Row.fromSeq(values)
        })
        split
      }
    }
  }
}
