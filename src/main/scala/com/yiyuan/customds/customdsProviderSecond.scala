package com.yiyuan.customds

import com.yiyuan.workingClass.{datasourceBase, datasourceBaseThree, datasourceBasetwo}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import scala.util.{Failure, Success, Try}

class customdsProviderSecond extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "ySource2"

  override def createRelation(sqlContextCR: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new datasourceBaseThree(sqlContextCR,
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
