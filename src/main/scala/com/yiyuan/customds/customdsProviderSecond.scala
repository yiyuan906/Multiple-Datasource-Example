package com.yiyuan.customds

import com.yiyuan.workingClass.datasourceBase
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class customdsProviderSecond extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "ySource2"

  override def createRelation(sqlContextCR: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val dsb = new datasourceBase(sqlContextCR, parameters("path"))
    dsb.relationReturn()
  }
}
