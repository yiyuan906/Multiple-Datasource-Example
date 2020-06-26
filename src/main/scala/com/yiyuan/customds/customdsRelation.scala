package com.yiyuan.customds

import java.io.Serializable

import com.yiyuan.customdsClasses.sourceSchema
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class customdsRelation(val relationContext:SQLContext, val relationPath:String)
  extends BaseRelation with PrunedScan with java.io.Serializable {

  /*val minioObj = new MinioClient(
    relationContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.endpoint"),
    relationContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.access.key"),
    relationContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.secret.key")
  )

  val splitone = relationPath.split("://")
  val splittwo = splitone(1).split("/")

  val objectData = minioObj.statObject(splittwo(0), splitone(1).split(splittwo(0) + "/")(1)).httpHeaders().get("X-Amz-Meta-User.datasourceClass")
  if (objectData == null)
    throw new Exception("Metadata for user.datasourceClass is empty")
  val metadata = objectData.get(0)*/

  val metadata = relationContext.getConf("metadata key")

  val classOfDs = Class.forName(metadata).newInstance().asInstanceOf[sourceSchema]
  val schemaFromClass = classOfDs.schemaRetrieved

  override def sqlContext: SQLContext = relationContext

  override def schema: StructType = schemaFromClass

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val metadata2 = relationContext.getConf("metadata key")
    val classOfDs2 = Class.forName(metadata2).newInstance().asInstanceOf[sourceSchema]
    val methodFromClass2 = classOfDs2.getClass.getDeclaredMethod("customparser", relationContext.getClass, new String().getClass)
    val returnedValue = methodFromClass2.invoke(classOfDs2, relationContext, relationPath).asInstanceOf[RDD[Row]]
    returnedValue
  }
}
