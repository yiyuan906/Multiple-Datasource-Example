package com.yiyuan.customds

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class customdsProviderSecond extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "ySource2"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val metadata = sqlContext.getConf("metadata key")
    val tClass = Class.forName(metadata)
    val method = tClass.getDeclaredMethod("relationReturn", sqlContext.getClass,new String().getClass)
    method.invoke(tClass.newInstance(), sqlContext, parameters("path")).asInstanceOf[BaseRelation]
  }
}
