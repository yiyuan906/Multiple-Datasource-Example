package com.yiyuan.workingClass

import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import scala.util.{Failure, Success, Try}

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
        var linecounter = 1
        val split = relationContext.sparkContext.textFile(relationPath).map(line =>{
          val Elem = line.split(" ")

          if(Elem.size == 19) {
            val values = requiredColumns.map {
              case "Date" => Try(Date.valueOf(Elem(0))) match {
                case Success(x) => x
                case Failure(e) => println(s"Date at line $linecounter received $e, setting to null"); null
              }
              case "Time" => Elem(1)
              case "cs-method" => Elem(2)
              case "cs-uri-stem" => Elem(3)
              case "cs-uri-query" => Elem(4)
              case "s-port" => Try(Elem(5).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"s-port at line $linecounter received $e, setting to null"); null
              }
              case "cs-username" => Elem(6)
              case "c-ip" => Elem(7)
              case "cs-version" => Elem(8)
              case "cs(User-Agent)" => Elem(9)
              case "cs(Cookie)" => Elem(10)
              case "cs(Referer)" => Elem(11)
              case "cs-host" => Elem(12)
              case "sc-status" => Try(Elem(13).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"sc-status at line $linecounter received $e, setting to null"); null
              }
              case "sc-substatus" => Try(Elem(14).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"sc-substatus at line $linecounter received $e, setting to null"); null
              }
              case "sc-win32-status" => Try(Elem(15).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"sc-win32-status at line $linecounter received $e, setting to null"); null
              }
              case "sc-bytes" => Try(Elem(16).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"sc-bytes at line $linecounter received $e, setting to null"); null
              }
              case "cs-bytes" => Try(Elem(17).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"cs-bytes at line $linecounter received $e, setting to null"); null
              }
              case "Time-taken" => Try(Elem(18).toInt) match {
                case Success(x) => x
                case Failure(e) => println(s"Time-taken at line $linecounter received $e, setting to null"); null
              }
            }
            linecounter+=1
            Row.fromSeq(values)
          }
          else
            throw new Exception("Data provided does not match datasource class given from metadata," +
              " data cannot be parsed.")
        })
        split
      }
    }
  }
}
