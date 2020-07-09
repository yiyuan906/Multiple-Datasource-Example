package com.yiyuan.workingClass

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import com.example.protos.persons._

class datasourceProtobufRelation {
  def relationReturn(relationContext:SQLContext, relationPath:String):BaseRelation = {
    new BaseRelation with PrunedScan {
      override def sqlContext: SQLContext = relationContext

      override def schema: StructType = new StructType()
        .add(StructField("name",StringType,true))
        .add(StructField("age",IntegerType,true))
        .add(StructField("gender",StringType,true))
        .add(StructField("tags",ArrayType(StringType),true))
        .add(StructField("addresses",ArrayType(new StructType()
          .add("street",StringType)
          .add("city",StringType))))

      override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        val split = relationContext.sparkContext.textFile(relationPath).map{ x=>
          val p = Person.parseFrom(org.apache.commons.codec.binary.Base64.decodeBase64(x))

          val values = requiredColumns.map {
            case "name" => p.getName
            case "age" => p.getAge
            case "gender" => p.getGender.name
            case "tags" => p.tags.toArray
            case "addresses" => p.addresses.toArray
          }

          Row.fromSeq(values)
        }

        split
      }
    }
  }
}
