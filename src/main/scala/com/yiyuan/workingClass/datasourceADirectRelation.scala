package com.yiyuan.workingClass

import java.io.StringWriter
import java.sql.Date

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.util.Try

class datasourceADirectRelation {
  def getStringStream(streamContext: SQLContext, streamPath: String):String = {
    val minioObj = new MinioClient(
      streamContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.endpoint"),
      streamContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.access.key"),
      streamContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.secret.key")
    )

    val splitone = streamPath.split("://")

    if(splitone.size==1 || splitone(0)!="s3a")
      throw new Exception("Please provide path in the following format s3a://bucket-name/object")
    val splittwo = splitone(1).split("/")

    if (!minioObj.bucketExists(splittwo(0)))
      throw new Exception("Bucket does not exist")

    val writer = new StringWriter()
    IOUtils.copy(minioObj.getObject(splittwo(0), splitone(1).split(splittwo(0) + "/")(1)),writer)
    writer.toString
  }

  def dfReturn(relationContext: SQLContext, relationPath: String): RDD[Row] = {
    val inputStreamString = getStringStream(relationContext, relationPath)

    val mapped = inputStreamString.split("\n").map{x=>
      println(x)
      val split = x.split(" ")
      val desc = x.drop((split(0) + split(1) + split(2) + split(3) + split(4) + split(5)).length + 6)
      Row(split(0),split(1), split(3), split(2), split(5).toInt, desc)
    }

    val paraed = relationContext.sparkContext.parallelize(mapped)

    paraed
  }
}
