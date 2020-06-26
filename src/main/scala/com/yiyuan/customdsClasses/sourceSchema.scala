package com.yiyuan.customdsClasses

import org.apache.spark.sql.types.StructType

abstract class sourceSchema {
  def schemaRetrieved:StructType
}
