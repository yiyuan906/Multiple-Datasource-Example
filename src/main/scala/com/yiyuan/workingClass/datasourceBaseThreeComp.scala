package com.yiyuan.workingClass

import java.io.StringWriter
import java.sql.Date

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class datasourceBaseThreeComp(compContext:SQLContext, columns: Array[String], compPath:String,
                              bucket:String,endpoint:String,accesskey:String,secretkey:String)
  extends RDD[Row](compContext.sparkContext, Nil) {
  def getStringStream(streamPath: String):String = {
    val minioObj = new MinioClient(endpoint,accesskey,secretkey)

    if (!minioObj.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    val writer = new StringWriter()
    IOUtils.copy(minioObj.getObject(bucket, streamPath),writer)
    writer.toString
  }

  def getMetadata(streamPath: String):String = {
    val minioObj = new MinioClient(endpoint,accesskey,secretkey)

    if (!minioObj.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    Try(minioObj.statObject(bucket, streamPath).httpHeaders().get("X-Amz-Meta-User.datasourceclass")) match {
      case Success(objData) =>
        if(objData == null)
          throw new Exception("Metadata for user.datasourceClass is empty")
        objData.get(0)
      case Failure(x) => throw new Exception("Object does not exist")
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val listOfObj = compPath.split(",")
    val streamsOfObj = listOfObj.map{ObjName=>
      (getStringStream(ObjName),getMetadata(ObjName),ObjName)
    }

    var metadataRef = new ArrayBuffer[String]

    val streamToLine = streamsOfObj.map{value=>
      val datastream = value._1
      val metadata = value._2
      val streamInArray = datastream.split("\n")
      if(value._3.endsWith("csv")) {                          //drop header row
        val filteredStream = streamInArray.drop(1)
        for(lines<-filteredStream)
          metadataRef+= metadata
        filteredStream
      }
      else {
        for(lines<-streamInArray)
          metadataRef+= metadata
        streamInArray
      }
    }

    val lineIterator = streamToLine.flatten.toIterator
    val metadataIterator = metadataRef.toArray.iterator

    import compContext.implicits._

    new Iterator[Row] {
      override def hasNext: Boolean = lineIterator.hasNext

      override def next(): Row = {
        val row = Try(Class.forName(metadataIterator.next())) match
        {
          case Success(classInstance) =>
            val method = classInstance.getDeclaredMethod("streamParsing", new String().getClass) //, new Array[String](0).getClass
            method.invoke(classInstance.newInstance(), lineIterator.next()).asInstanceOf[Row]
          case Failure(x) =>
            throw new Exception(s"Class cannot be found")
        }

        val values = columns.map {
          case "Date" => Date.valueOf(row.getString(0))
          case "Time" => row.getString(1)
          case "Action" => row.getString(2)
          case "Role" => row.getString(3)
          case "ActionID" => row.getInt(4)
          case "Description" => row.getString(5)
        }
        Row.fromSeq(values)
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(partitionsZ(0))
}

case class partitionsZ(idx: Int) extends Partition {
  override def index: Int = idx
}