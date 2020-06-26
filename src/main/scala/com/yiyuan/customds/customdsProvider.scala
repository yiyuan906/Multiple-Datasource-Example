package com.yiyuan.customds

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class customdsProvider extends DataSourceRegister with RelationProvider{
  override def shortName(): String = "yySource"

  override def createRelation(providerContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new customdsRelation(providerContext, parameters("path"))
  }
}
