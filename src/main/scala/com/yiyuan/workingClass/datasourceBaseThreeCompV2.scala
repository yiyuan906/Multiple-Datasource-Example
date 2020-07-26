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

class datasourceBaseThreeCompV2(compContext:SQLContext, columns: Array[String], compPath:String,
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
      val writer = new StringWriter ()
      IOUtils.copy(minio.getObject (bucket, streamPath), writer)
      writer.toString
    }
  }

  def getMetadata(streamPath: String, minio:MinioClient):String = {
    if (!minio.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    Try(minio.statObject(bucket, streamPath).httpHeaders().get("X-Amz-Meta-User.datasourceclass")) match {
      case Success(objData) =>
        if(objData == null)
          throw new Exception("Metadata for user.datasourceClass is empty")
        objData.get(0)
      case Failure(x) => throw new Exception("Object does not exist")
    }
  }

  def getObjList(streamPath:String, minio:MinioClient):Array[String] = {
    if(readMode == "specific"){           //expects list to be given seperated by commas "bucket/dir/obj,bucket/dir/obj"
      streamPath.split(",").map{path=>{
          path.drop(bucket.size+1)
        }
      }
    }
    else {                                //reads all specified
      val objite = minio.listObjects(bucket,streamPath.drop(bucket.size+1)).iterator()
      val listBuffer = new ArrayBuffer[String]
      while(objite.hasNext)
        listBuffer += objite.next().get().objectName()
      listBuffer.toArray[String]
    }
  }

  def getRow(currentClass:String, currentObjectName:String, currentObject:AnyRef):Row = {
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
      val line = currentObject.asInstanceOf[String]
      Try(Class.forName(currentClass)) match {
      case Success (classInstance) =>
        val method = classInstance.getDeclaredMethod ("streamParsing", new String().getClass) //, new Array[String](0).getClass
        method.invoke (classInstance.newInstance (),line).asInstanceOf[Row]
      case Failure (x) =>
        throw new Exception(s"Class $currentClass cannot be found")
      }
    }
  }

  var metadataRef = new ArrayBuffer[(String,String)]

  def getTextStream(lineStream:Array[(Any,String,String)]):Iterator[String] = {
    lineStream.flatMap { value =>
      val datastream = value._1.toString
      val metadata = value._2
      val objName = value._3
      val streamInArray = datastream.split("\n")
      if (value._3.endsWith("csv")) { //drop header row
        val filteredStream = streamInArray.drop(1)
        for (lines <- filteredStream)
          metadataRef += Tuple2(metadata,objName)
        filteredStream
      }
      else {
        for (lines <- streamInArray)
          metadataRef += Tuple2(metadata, objName)
        streamInArray
      }
    }.toIterator
  }

  def getSingleStream(singleStream:Array[(Any,String,String)]):Iterator[ParquetReader[Group]] = {
    singleStream.map { value =>
      val metadata = value._2
      val objName = value._3
      metadataRef += Tuple2(metadata, objName)
      val data = value._1.asInstanceOf[ParquetReader[Group]]
      data
    }.iterator
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val minioObj = new MinioClient(endpoint, accesskey, secretkey)
    val listOfObj = getObjList(compPath, minioObj)
    val streamsOfObj = listOfObj.map { ObjName =>
      (getStream(ObjName, minioObj), getMetadata(ObjName, minioObj), ObjName)
    }

    val textStream = streamsOfObj.filter{ x =>
      if(x._3.endsWith(".txt") || x._3.endsWith(".csv"))
        true
      else false
    }

    textStream.foreach(x=>println("part1" + x._3))

    val textStreamIte = getTextStream(textStream)

    val lineStream = streamsOfObj.filter{ x =>
      if(x._3.endsWith(".txt") || x._3.endsWith(".csv"))
        false
      else true
    }

    lineStream.foreach(x=>println("part2" + x._3))

    val lineStreamIte = getSingleStream(lineStream)

    val lineIterator:Iterator[AnyRef] = textStreamIte ++ lineStreamIte

    val metadataIterator = metadataRef.toArray.iterator

    import compContext.implicits._

    var curClass = ""
    var curObjName = ""
    var curObj:AnyRef = ""
    var wayOfRead = ""
    var transition = false

    new Iterator[Row] {
      override def hasNext: Boolean = {
        wayOfRead = Try(Class.forName(curClass)) match {
          case Success(classInstance) =>
            val method = classInstance.getDeclaredMethod("methodOfReading")
            method.invoke(classInstance.newInstance()).asInstanceOf[String]
          case Failure(x) =>
            "Line"
        }
        wayOfRead match {
          case "Line" => {
            if (metadataIterator.hasNext) {
              if (transition == false) {
                val nextSet = metadataIterator.next()
                curClass = nextSet._1
                curObjName = nextSet._2
                curObj = lineIterator.next()
              }
              else transition = false
              true
            }
            else
              false
          }
          case "Stream" => true
        }
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
          Try(metadataIterator.next()) match {
            case Success(nextSet) =>
              curClass = nextSet._1
              curObjName = nextSet._2
              curObj = lineIterator.next ()
              transition = true
              Row(null,null,null,null,null,null)
            case Failure(x) => {
              curClass = ""
              Row(null, null, null, null, null, null)
            }
          }
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(partitionsQ(0))
}

case class partitionsQ(idx: Int) extends Partition {
  override def index: Int = idx
}