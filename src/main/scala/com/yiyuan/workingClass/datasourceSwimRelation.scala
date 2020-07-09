package com.yiyuan.workingClass

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SQLContext}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.util.{Failure, Success, Try}

class datasourceSwimRelation {
  def relationReturn(relationContext:SQLContext, relationPath:String):BaseRelation = {
    new BaseRelation with PrunedScan with java.io.Serializable {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("activity_type",StringType,true))
        .add(StructField("Timestamp",TimestampType,true))
        .add(StructField("Favorite",BooleanType,true))
        .add(StructField("Title",StringType,true))
        .add(StructField("Distance",IntegerType,true))              //5
        .add(StructField("Calories",StringType,true))
        .add(StructField("Time",StringType,true))
        .add(StructField("Avg_pace",StringType,true))
        .add(StructField("Best_pace", StringType,true))
        .add(StructField("Flow",StringType,true))                   //10
        .add(StructField("total_strokes", IntegerType, true))
        .add(StructField("avg_swolf", IntegerType, true))
        .add(StructField("avg_stroke_rate", DoubleType,true))
        .add(StructField("min_temp",DoubleType,true))
        .add(StructField("Decompression",StringType,true))           //15
        .add(StructField("Best_lap_time",StringType,true))
        .add(StructField("Num_of_runs",IntegerType,true))

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        import relationContext.implicits._
        var linecounter = 1
        val split = relationContext.read.option("header",true).option("delimiter",";")
          .csv(relationPath)
          .map(x=>x.toString()).rdd
          .map{ line=>{
            val split = line.split(",")

            if(split.size==18) {
              val values = requiredColumns.map {
                case "activity_type" => split(0).drop(1)
                case "Timestamp" => {
                  val format = new SimpleDateFormat("dd/MM/yyyy HH:mm")
                  Try(new Timestamp(format.parse(split(1)).getTime)) match {
                    case Success(x) => x
                    case Failure(e) => println(s"Timestamp at line $linecounter received $e, setting to null."); null
                  }
                }
                case "Favorite" => Try(split(2).toBoolean) match {
                  case Success(x) => x
                  case Failure(e) => println(s"Favorite at line $linecounter received $e, setting to null"); null
                }
                case "Title" => split(3)
                case "Distance" => Try((split(4) + split(5)).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"Distance at line $linecounter received $e, setting to null"); null
                }
                case "Calories" => split(6)
                case "Time" => split(7)
                case "Avg_pace" => split(8)
                case "Best_pace" => split(9)
                case "Flow" => split(10)
                case "total_strokes" => Try(split(11).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"total_strokes at line $linecounter received $e, setting to null"); null
                }
                case "avg_swolf" => Try(split(12).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"avg_swolf at line $linecounter received $e, setting to null"); null
                }
                case "avg_stroke_rate" => Try(split(13).toDouble) match {
                  case Success(x) => x
                  case Failure(e) => println(s"avg_stroke_rate at line $linecounter received $e, setting to null"); null
                }
                case "min_temp" =>
                  Try(split(14).toDouble) match {
                    case Success(x) => x
                    case Failure(e) => println(s"min_temp at line $linecounter received $e, setting to null"); null
                  }
                case "Decompression" => split(15)
                case "Best_lap_time" => split(16)
                case "Num_of_runs" => Try(split(17).dropRight(1).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"Num_of_runs at line $linecounter received $e, setting to null"); null
                }
              }
              linecounter += 1
              Row.fromSeq(values)
            }
            else if(split.size == 17) {
              val values = requiredColumns.map {
                case "activity_type" => split(0).drop(1)
                case "Timestamp" => {
                  val format = new SimpleDateFormat("dd/MM/yyyy HH:mm")
                  Try(new Timestamp(format.parse(split(1)).getTime)) match {
                    case Success(x) => x
                    case Failure(e) => println(s"Timestamp at line $linecounter received $e, setting to null."); null
                  }
                }
                case "Favorite" => Try(split(2).toBoolean) match {
                  case Success(x) => x
                  case Failure(e) => println(s"Favorite at line $linecounter received $e, setting to null"); null
                }
                case "Title" => split(3)
                case "Distance" => Try((split(4)).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"Distance at line $linecounter received $e, setting to null"); null
                }
                case "Calories" => split(5)
                case "Time" => split(6)
                case "Avg_pace" => split(7)
                case "Best_pace" => split(8)
                case "Flow" => split(9)
                case "total_strokes" => Try(split(10).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"total_strokes at line $linecounter received $e, setting to null"); null
                }
                case "avg_swolf" => Try(split(11).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"avg_swolf at line $linecounter received $e, setting to null"); null
                }
                case "avg_stroke_rate" => Try(split(12).toDouble) match {
                  case Success(x) => x
                  case Failure(e) => println(s"avg_stroke_rate at line $linecounter received $e, setting to null"); null
                }
                case "min_temp" =>
                  Try(split(13).toDouble) match {
                    case Success(x) => x
                    case Failure(e) => println(s"min_temp at line $linecounter received $e, setting to null"); null
                  }
                case "Decompression" => split(14)
                case "Best_lap_time" => split(15)
                case "Num_of_runs" => Try(split(16).dropRight(1).toInt) match {
                  case Success(x) => x
                  case Failure(e) => println(s"Num_of_runs at line $linecounter received $e, setting to null"); null
                }
              }
              linecounter += 1
              Row.fromSeq(values)
            }
            else
              throw new Exception("Data provided does not match datasource class given from metadata," +
                " data cannot be parsed.")
          }}

        //split.filter(x=> !x.toString().contains("null"))      //possible filter for data
        split
      }
    }
  }
}
