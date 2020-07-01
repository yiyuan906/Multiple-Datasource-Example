package com.yiyuan.customdsClasses
import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.unix_timestamp

class datasourceCusloader1 extends sourceSchema {
  override def schemaRetrieved: StructType = new StructType()
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

  def customparser(sparkSess:SparkSession, path:String):sql.DataFrame = {
    import sparkSess.implicits._
    //val headers = "date,Time,cs-method,cs-uri-stem,cs-uri-query,s-port,cs-username,c-ip,cs-version,cs(User-Agent),cs(Cookie),cs(Referer),cs-host,sc-status,sc-substatus,sc-win32-status,sc-bytes,cs-bytes,time-taken"
    val headers = "F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,F13,F14,F15,F16,F17,F18,F19"
    val testD = sparkSess.read.textFile("ISSLogfileedit.txt").map(x=>{
      val Elem = x.split(" ")
      (Date.valueOf(Elem(0)), Elem(1),Elem(2), Elem(3)
        ,Elem(4),Elem(5).toInt,Elem(6),Elem(7),Elem(8),Elem(9),Elem(10),Elem(11),
        Elem(12),Elem(13).toInt,Elem(14).toInt,Elem(15).toInt,Elem(16).toInt,Elem(17).toInt,
        Elem(18).toInt)
    }).toDF(headers.split(","): _*)

    testD
  }
}
