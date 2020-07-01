package com.yiyuan.customds

import java.io.Serializable

import com.yiyuan.customdsClasses.{rType, sourceSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.StructType

class customdsRelationSecond(val relationContext:SQLContext, val relationPath:String)
  extends BaseRelation with PrunedScan with java.io.Serializable  {
  val metadata = relationContext.getConf("metadata key")

  val classOfDs = Class.forName(metadata).newInstance().asInstanceOf[sourceSchema]
  val schemaFromClass = classOfDs.schemaRetrieved

  override def sqlContext: SQLContext = relationContext

  override def schema: StructType = schemaFromClass

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val metadata2 = relationContext.getConf("metadata key")
    val classOfDs2 = Class.forName(metadata2).newInstance().asInstanceOf[sourceSchema]
    val fType = Class.forName(metadata2).newInstance().asInstanceOf[rType]
    //val methodT = classOfDs2.getClass().getDeclaredMethod("parseM", new String().getClass)

    val rType = fType.filetype match {
      case "text" => {
        val qType = relationContext.sparkContext.textFile(relationPath).map{x=>
          val methodT = classOfDs2.getClass().getDeclaredMethod("parseM", new String().getClass)
          val temp = x
          val returnT = methodT.invoke(classOfDs2, temp).asInstanceOf[Row]
          returnT
        }
        qType
      }
      case _ => {
        throw new Exception("File type specified in class not found.")
      }
    }
    println(rType.first().toString())
    rType
    /*val methodFromClass2 = classOfDs2.getClass.getDeclaredMethod("customparser", relationContext.getClass, new String().getClass)
    val returnedValue = methodFromClass2.invoke(classOfDs2, relationContext, relationPath).asInstanceOf[RDD[Row]]
    returnedValue*/
  }
}
