package com.yiyuan.workingClass

import java.io.StringWriter
import java.sql.Date

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import scala.collection.mutable.Seq


class datasourceBaseThreeCompV3(compContext:SQLContext, columns: Array[String], compPath:String,
                              readMode:String,endpoint:String,accesskey:String,secretkey:String)
  extends RDD[Row](compContext.sparkContext, Nil) {

  val bucket = compPath.split("/")(0)

  def getStream(streamPath: String, minio:MinioClient):Any = {
    if (!minio.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    if(streamPath.endsWith("parquet")){
      val hConf = new Configuration()
      hConf.set("fs.s3a.endpoint", endpoint)
      hConf.set("fs.s3a.access.key", accesskey)
      hConf.set("fs.s3a.secret.key", secretkey)
      val path = new Path(s"s3a://$bucket/$streamPath")
      val parReader = ParquetReader.builder(new GroupReadSupport(),path).withConf(hConf).build()
      parReader
    }
    else {
      val minioStream = minio.getObject(bucket,streamPath)
      if(streamPath.endsWith("csv")) {
        val Ite = IOUtils.readLines(minioStream,"UTF-8").iterator()
        Ite.next()
        Ite
      }
      else
        IOUtils.readLines(minioStream,"UTF-8").iterator()
    }
  }

  def getMetadata(streamPath: String, minio:MinioClient):String = {
    if (!minio.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    Try(minio.statObject(bucket, streamPath).httpHeaders().get("X-Amz-Meta-User.datasourceclass")) match {
      case Success(objData) =>
        if(objData == null)
          "Empty" //throw new Exception("Metadata for user.datasourceClass is empty")
        else
          objData.get(0)
      case Failure(x) => throw new Exception("Object does not exist")
    }
  }

  def getObjList(streamPath:String, minio:MinioClient):Array[String] = {
    if(readMode == "specific"){           //expects list to be given seperated by commas "bucket/dir/obj,bucket/dir/obj"
      streamPath.split(",").map{path=>
        path.drop(bucket.size+1)
      }
    }
    else {                                //reads all specified
      val objite = minio.listObjects(bucket,streamPath.drop(bucket.size+1)).iterator()
      val listBuffer = new ArrayBuffer[String]
      while(objite.hasNext)
        listBuffer += objite.next().get().objectName()

      listBuffer.foreach(println)

      listBuffer.toArray[String]
    }
  }

  def getRow(currentClass:String, currentObjectName:String, currentObject:Any):Row = {
    if(currentObjectName.endsWith(".parquet")) {
      val parStream = currentObject.asInstanceOf[ParquetReader[Group]]
      Try(Class.forName(currentClass)) match {
        case Success(classInstance) =>
          val method = classInstance.getDeclaredMethod("singleStream", parStream.getClass)
          Try(method.invoke(classInstance.newInstance(),parStream).asInstanceOf[Row]) match {
            case Success(row) => row
            case Failure(x) => Row.empty
          }
        case Failure(x) =>
          throw new Exception(s"Class $x cannot be found")
      }
    }
    else {
      val line = currentObject.asInstanceOf[java.util.Iterator[String]]
      Try(Class.forName(currentClass)) match {
      case Success (classInstance) =>
        val method = classInstance.getDeclaredMethod ("streamParsing", new String().getClass) //, new Array[String](0).getClass
        Try(method.invoke(classInstance.newInstance (),line.next()).asInstanceOf[Row]) match {
          case Success(row) => row
          case Failure(x) => Row.empty
        }
      case Failure (x) =>
        throw new Exception(s"Class $currentClass cannot be found")
      }
    }
  }

  var metadataRef = new ArrayBuffer[(String,String)]

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val minioObj = new MinioClient(endpoint, accesskey, secretkey)
    val listOfObj = getObjList(compPath, minioObj)
    val fullDetailsOfObjs = listOfObj.map { ObjName =>
      (getStream(ObjName, minioObj), getMetadata(ObjName, minioObj), ObjName)
    }

    println("\nObjects excluded from read:")
    println("="*30)
    val filteredFullDetailsOfObjs = fullDetailsOfObjs.filter(x=> {        //filter objects with empty metadata
      if (x._2.contains("Empty")) {
        println(x._3)
        false
      }
      else
        true
    })
    println("="*30)
    val (arrayOfObjs,metadataOfObjs,objName) = filteredFullDetailsOfObjs.unzip3

    val iteOfObjs = arrayOfObjs.iterator
    val iteOfMetadata = metadataOfObjs.iterator
    val iteOfObjName = objName.iterator

    import compContext.implicits._

    //initialised variable for Iterator[Row]
    var curClass = iteOfMetadata.next()
    var curObjName = iteOfObjName.next()
    var curObj:Any = iteOfObjs.next()

    new Iterator[Row] {
      override def hasNext: Boolean = {
        if (curClass != null) {
          if (curObjName.endsWith(".parquet"))
           true
          else {    //text type
           if(curObj.asInstanceOf[java.util.Iterator[String]].hasNext)
             true
           else
             iteOfObjs.hasNext
          }
        }
        else
          false
      }

      override def next(): Row = {
        val row = getRow(curClass, curObjName, curObj)

        if(row != Row.empty) {
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
        else {
          Try(curObj = iteOfObjs.next()) match {
            case Success(y) =>
              curClass = iteOfMetadata.next()
              curObjName = iteOfObjName.next()
              val nextRow = getRow(curClass,curObjName,curObj)
              val values = columns.map {
                case "Date" => Date.valueOf(nextRow.getString(0))
                case "Time" => nextRow.getString(1)
                case "Action" => nextRow.getString(2)
                case "Role" => nextRow.getString(3)
                case "ActionID" => nextRow.getInt(4)
                case "Description" => nextRow.getString(5)
              }
              Row.fromSeq(values)
            case Failure(x) => {
              println("entered fail")
              curClass = null
              Row(null, null, null, null, null, null)
            }
          }
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(partitionsW(0))
}

case class partitionsW(idx: Int) extends Partition {
  override def index: Int = idx
}