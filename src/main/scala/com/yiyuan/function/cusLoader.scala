package com.yiyuan.function

import com.yiyuan.customdsClasses.sourceSchema
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class cusLoader(val SparkSess:SparkSession) {
  val Spark = this.SparkSess

  def getMetaData(relationPath:String): String ={
    val minioObj = new MinioClient(
      Spark.sparkContext.getConf.get("spark.hadoop.fs.s3a.endpoint"),
      Spark.sparkContext.getConf.get("spark.hadoop.fs.s3a.access.key"),
      Spark.sparkContext.getConf.get("spark.hadoop.fs.s3a.secret.key")
    )

    val splitone = relationPath.split("://")
    val splittwo = splitone(1).split("/")

    val objectData = minioObj.statObject(splittwo(0), splitone(1).split(splittwo(0) + "/")(1)).httpHeaders().get("X-Amz-Meta-User.datasourceClass")
    if (objectData == null)
      throw new Exception("Metadata for user.datasourceClass is empty")
    val metadata = objectData.get(0)
    metadata
  }

  def load(path:String, metadataPlaceHolder:String):sql.DataFrame = {
    //schema retrieve
    val metadata = metadataPlaceHolder //getMetaData(path)
    val classOfDs = Class.forName(metadata).newInstance().asInstanceOf[sourceSchema]
    val schemaFromClass = classOfDs.schemaRetrieved

    //data retrieve
    //data returned already has a schema due to the type casting that is needed to fit a schema
    val methodFromClass = classOfDs.getClass.getDeclaredMethod("customparser", Spark.getClass, new String().getClass)
    val returnedData = methodFromClass.invoke(classOfDs, Spark, path).asInstanceOf[sql.DataFrame]

    //creating DF from schema retrieved and data
    val combine = Spark.createDataFrame(returnedData.rdd, schemaFromClass)
    combine
  }
}
