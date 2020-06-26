package com.yiyuan.customdsClasses

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

class datasourceISSLog extends sourceSchema with java.io.Serializable {
  override def schemaRetrieved: StructType = new StructType()
    .add(StructField("Date",StringType,true))
    .add(StructField("Time",StringType, true))
    .add(StructField("cs-method", StringType, true))
    //.add(StructField("cs-uri-stem", StringType, true))
    .add(StructField("cs-uri-query", StringType, true))
    //.add(StructField("s-port", IntegerType, true))      //int
    .add(StructField("cs-username", StringType, true))
    //.add(StructField("c-ip", StringType,true))
    //.add(StructField("cs-version",StringType,true))
    /*.add(StructField("cs(User-Agent)",StringType,true))
    .add(StructField("cs(Cookie)",StringType,true))
    .add(StructField("cs(Referer)",StringType,true))
    .add(StructField("cs-host",StringType,true))
    .add(StructField("sc-status",StringType,true))    //int
    // .add(StructField("sc-substatus",StringType,true)) //int
    // .add(StructField("sc-win32-status",StringType,true)) //int
    .add(StructField("sc-bytes", StringType,true)) //int
    .add(StructField("cs-bytes", StringType,true)) //int*/
    .add(StructField("Time-taken", IntegerType,true)) //int

  def checkToNull(valueA:String):String = {
    if(valueA == "/" || valueA == "-")
      null
    else
      valueA
  }

  def customparser(sparkSess:SQLContext, path:String):RDD[Row] = {
    import sparkSess.implicits._

    val splitQ = sparkSess.sparkContext.textFile(path).map(line =>{
      val Elem = line.split(" ")
      Row(Elem(0), Elem(1),Elem(2), Elem(4), Elem(6),
        // ,Elem(4),Elem(5),Elem(7),Elem(8),Elem(9),Elem(10),Elem(11), Elem(12),Elem(13),Elem(16),Elem(17),
       Elem(18).toInt)
    })
    splitQ
  }
}
