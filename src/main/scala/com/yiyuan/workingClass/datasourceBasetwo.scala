package com.yiyuan.workingClass

import java.sql.Date

import io.minio.MinioClient
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class datasourceBasetwo(relationContext:SQLContext, relationPath:String) {
  def getMetadata(relationPath: String):String = {
    val minioObj = new MinioClient(
      relationContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.endpoint"),
      relationContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.access.key"),
      relationContext.sparkContext.getConf.get("spark.hadoop.fs.s3a.secret.key")
    )

    val splitone = relationPath.split("://")

    if(splitone.size==1 || splitone(0)!="s3a")
      throw new Exception("Please provide path in the following format s3a://bucket-name/object")
    val splittwo = splitone(1).split("/")

    if (!minioObj.bucketExists(splittwo(0)))
      throw new Exception("Bucket does not exist")

    Try(minioObj.statObject(splittwo(0), splitone(1).split(splittwo(0) + "/")(1)).httpHeaders().get("X-Amz-Meta-User.datasourceclass")) match {
      case Success(objData) =>
        if(objData == null)
          throw new Exception("Metadata for user.datasourceClass is empty")
        objData.get(0)
      case Failure(x) => throw new Exception("Object does not exist")
    }
  }

  private var filterMode = 1        // 1 = single, 2 = multiple(testfile_*.txt), 3 = all (* or *.filetype or dir given)

  def dirCheck(path:String):String = { //truncates to directory, set mode done for filter as well
    println(path.dropRight(path.split("/").last.length))
    if(path.split("/").last.split("\\.")(0).length>2
      && path.split("/").last.contains("*")){      //checks for filename_*.filetype input, size>2 asserts filename_*.
      filterMode = 2
      path.dropRight(path.split("/").last.length)
    }
    else if(path.endsWith("/")) {            //assumes a directory
      filterMode = 3
      path
    }
    else if(path.split("/").last.startsWith("*")){            //assumes a directory (* or *.filetype)
      filterMode = 3
      path.dropRight(path.split("/").last.length)
    }
    else {                                          //assumes a single file
      filterMode = 1
      path.dropRight(path.split("/").last.length)
    }
  }

  def fileFilter(path:String):Boolean = {
    filterMode match {
      case 1 => {
        path.split("/").last == relationPath.split("/").last //checks same file name
      }
      case 2 => {
        path.split("/").last.contains(relationPath.split("/").last.split("\\.")(0).dropRight(2)) //drop _* to filter
      }
      case 3 => {                               //all files pass
        true
      }
    }
  }

  def extensionFilter(path:String):Boolean = {
    if(relationPath.split("/").last.split("\\.").length>1)
      path.split("/").last.split("\\.")(1) == relationPath.split("/").last.split("\\.")(1)
    else                                          //assumes * is given
      true
  }

  def listToIterator(path:Path):Iterator[String] = {
    val fs = path.getFileSystem(relationContext.sparkContext.hadoopConfiguration)
    fs.listStatus(path).map{file=>
      file.getPath.toString
    }.filter(
      fileFilter(_)
    ).filter(
        extensionFilter(_)
      ).iterator
  }

  private val StructEmptyFrame = new StructType()
    .add(StructField("Date",DateType))
    .add(StructField("Time",StringType))
    .add(StructField("Action",StringType))
    .add(StructField("Role",StringType))
    .add(StructField("ActionID",IntegerType))
    .add(StructField("Description",StringType))

  def relationReturn():BaseRelation = {
    val fsPath = new Path(dirCheck(relationPath))          //gives directory

    val fileListIterator = listToIterator(fsPath)                    //fileList.iterator

    var rddArray = new ArrayBuffer[RDD[Row]]

    new BaseRelation with PrunedScan {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("Date",DateType))
        .add(StructField("Time",StringType))
        .add(StructField("Action",StringType))
        .add(StructField("Role",StringType))
        .add(StructField("ActionID",IntegerType))
        .add(StructField("Description",StringType))

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        while(fileListIterator.hasNext) {
          val file = fileListIterator.next()
          println(file)
          val classpathMetadata = getMetadata(file)

          Try(Class.forName(classpathMetadata)) match
          {
            case Success(classInstance) =>
              val method = classInstance.getDeclaredMethod("dfReturn", sqlContext.getClass, new String().getClass)
              rddArray += method.invoke(classInstance.newInstance(), relationContext, file).asInstanceOf[RDD[Row]]
            case Failure(x) =>
              throw new Exception(s"Class $classpathMetadata cannot be found")
          }
        }

        import relationContext.implicits._

        val combinedRDD = rddArray.toList.fold( //joining of data (requires same schema)
          relationContext.createDataFrame(relationContext.sparkContext.emptyRDD[Row], StructEmptyFrame).rdd
        )((first,second)=>first.union(second))

        combinedRDD.map{ row=> {
          val values = requiredColumns.map {
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
    }
  }
}
