package com.yiyuan.customdsClasses
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class datasourceTestTxt extends sourceSchema with java.io.Serializable {

  override def schemaRetrieved:StructType = new StructType()
    .add(StructField("IntField", IntegerType, true))
    .add(StructField("StringField", StringType, true))
    .add(StructField("IntFieldO",IntegerType, true))

  def customparser(sparkSess:SQLContext, path:String):RDD[Row] = {
    import sparkSess.implicits._
    val converted = sparkSess.sparkContext.textFile(path).map(line=>{
      val split = line.split(",")
      Row(split(0).toInt,split(1),split(2).toInt)
    })
    converted
  }
}
