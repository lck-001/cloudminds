package com.cloudminds.udf

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

/**
 * @Author jun.luo
 * @create 2021/7/20 11:11
 */
class TestUdf extends GenericUDF{
  override def initialize(objectInspectors: Array[ObjectInspector]): ObjectInspector = ???

  override def evaluate(deferredObjects: Array[GenericUDF.DeferredObject]): AnyRef = ???

  override def getDisplayString(strings: Array[String]): String = ???
}
