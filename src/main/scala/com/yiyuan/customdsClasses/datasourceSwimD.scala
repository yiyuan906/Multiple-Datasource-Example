package com.yiyuan.customdsClasses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}

class datasourceSwimD extends sourceSchema {

  override def schemaRetrieved: StructType = new StructType()
    .add(StructField("open_water_swimming",StringType,true))
    .add(StructField("Timestamp",StringType,true))
    .add(StructField("Favorite",BooleanType,true))
    .add(StructField("Title",StringType,true))
    .add(StructField("Distance",IntegerType,true))              //5
    .add(StructField("Calories",StringType,true))
    .add(StructField("Time",StringType,true))
    .add(StructField("Avg pace",StringType,true))
    .add(StructField("Best pace", StringType,true))
    .add(StructField("Flow",StringType,true))                   //10
    .add(StructField("total_strokes", IntegerType, true))
    .add(StructField("avg_swolf", IntegerType, true))
    .add(StructField("avg_stroke_rate", IntegerType,true))
    .add(StructField("min_temp",DoubleType,true))
    .add(StructField("Decompression",StringType,true))           //15
    .add(StructField("Best_lap_time",StringType,true))
    .add(StructField("Num_of_runs",IntegerType))

  def customparser(sparkSess:SQLContext, path:String):RDD[Row] = {
    import sparkSess.implicits._
    val split = sparkSess.read.option("header",true).option("delimiter",";")
      .csv("swimming.csv")
      .map(x=>x.toString())
      .map{ line=>{
        val split = line.split(",")
        (
          split(0),split(1),split(2).toBoolean,split(3),Integer.parseInt(split(4)+split(5)),split(6),
          split(7),split(8),split(9),split(10),split(11).toInt,split(12).toInt,split(13).toInt,split(14).toDouble,split(15),split(16)
          ,split(17).dropRight(1).toInt
        )
      }}.toDF()

    println(split.first().toString())
    split.rdd
  }
}
