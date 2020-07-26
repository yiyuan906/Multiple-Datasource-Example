package com.yiyuan.FinalOne.customds

import com.yiyuan.workingClass.datasourceBaseThree
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import scala.util.{Failure, Success, Try}

class customdsProvider extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "CDS"

  override def createRelation(sqlContextCR: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new datasourceRelation(sqlContextCR,
      parameters("path"),
      Try(parameters("readMode")) match {
        case Success(output) => output
        case Failure(x) => ""
      },
      parameters("endpoint"),
      parameters("accesskey"),
      parameters("secretkey")
      )
  }
}
