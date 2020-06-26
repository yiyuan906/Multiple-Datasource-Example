package com.yiyuan.customdsClasses
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}

class datasourceCusloader2 extends sourceSchema {
  override def schemaRetrieved: StructType = new StructType()
    .add(StructField("open_water_swimming",StringType,true))
    .add(StructField("Timestamp",StringType,true))
    .add(StructField("Favorite",BooleanType,true))
    .add(StructField("Title",StringType,true))
    .add(StructField("Distance",IntegerType,true))
    .add(StructField("Calories",IntegerType,true))
    .add(StructField("Time",StringType,true))
    .add(StructField("Avg_pace",StringType,true))
    .add(StructField("Best_pace", StringType,true))
    .add(StructField("Flow",StringType,true))
    .add(StructField("total_strokes", IntegerType, true))
    .add(StructField("avg_swolf", IntegerType, true))
    .add(StructField("avg_stroke_rate", IntegerType,true))
    .add(StructField("min_temp",DoubleType,true))
    .add(StructField("Decompression",StringType,true))
    .add(StructField("Best_lap_time",StringType,true))
    .add(StructField("Num_of_runs",IntegerType))

  def customparser(sparkSess:SparkSession, path:String):sql.DataFrame = {
    import sparkSess.implicits._
    val headers = "open_water_swimming,Timestamp,Favorite,Title,Distance,Calories,Time,Avg_pace," +
      "Best_pace,Flow,total_strokes,avg_swolf,avg_stroke_rate,min_temp,Decompression,Best_lap_time,Num_of_runs"
    val df = sparkSess.read.option("header",true).option("delimiter",";")
      .csv("swimming.csv")
      .map(x=>x.toString())
      .map{x=>{
        val Elem = x.split(",")
        (
          Elem(0).drop(1),Elem(1),Elem(2).toBoolean,Elem(3),Integer.parseInt(Elem(4)+Elem(5)),Integer.parseInt(Elem(6)),
          Elem(7),Elem(8),Elem(9),Elem(10),Elem(11).toInt,Elem(12).toInt,Elem(13).toInt,Elem(14).toDouble,Elem(15),Elem(16)
          ,Elem(17).dropRight(1).toInt
        )
      }}.toDF(headers.split(","): _*)
    df
  }
}
