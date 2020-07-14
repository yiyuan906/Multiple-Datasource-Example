package com.yiyuan.workingClass

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

class datasourceBaseThree(relationContext:SQLContext, relationPath:String, bucket:String, endpoint:String, accesskey:String,
                          secretkey:String) extends BaseRelation with PrunedScan{
  override def sqlContext: SQLContext = relationContext

  override def schema: StructType = new StructType()
    .add(StructField("Date",DateType))
    .add(StructField("Time",StringType))
    .add(StructField("Action",StringType))
    .add(StructField("Role",StringType))
    .add(StructField("ActionID",IntegerType))
    .add(StructField("Description",StringType))

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new datasourceBaseThreeComp(relationContext,requiredColumns,relationPath,bucket,endpoint,accesskey,secretkey)
}
