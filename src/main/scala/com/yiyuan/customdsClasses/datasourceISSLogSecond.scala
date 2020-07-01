package com.yiyuan.customdsClasses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class datasourceISSLogSecond extends sourceSchema with rType with java.io.Serializable {
  override def schemaRetrieved: StructType = new StructType()
    .add(StructField("Date",StringType,true))
    .add(StructField("Time",StringType, true))
    .add(StructField("cs-method", StringType, true))
    .add(StructField("cs-uri-query", StringType, true))
    .add(StructField("s-port", StringType, true))      //int
    .add(StructField("cs-username", StringType, true))
    .add(StructField("Time-taken", IntegerType,true)) //int

  override val filetype: String = "text"

  def parseM(line:String):Row = {
    val split = line.split(" ")
    //println(split(2),split(1),split(0),split(6),split(4),split(18).toInt)
    Row(split(2),split(1),split(6),split(0),split(18).toInt,split(5),split(4))
  }
}
