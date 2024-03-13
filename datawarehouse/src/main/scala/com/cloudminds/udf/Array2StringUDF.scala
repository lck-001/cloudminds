package com.cloudminds.udf

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}
import  scala.collection.JavaConverters._

class Array2StringUDF extends GenericUDF{
  private var param1:StringObjectInspector = _
  private var param2:StringObjectInspector = _
  override def initialize(objectInspectors: Array[ObjectInspector]): ObjectInspector = {
    if (objectInspectors.length != 2) throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 2 arguments.")
    if (objectInspectors(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory != PrimitiveCategory.STRING && objectInspectors(1).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory != PrimitiveCategory.STRING){
      throw new UDFArgumentException("The function argument need string.")
    }
    param1 = objectInspectors(0).asInstanceOf[StringObjectInspector]
    param2 = objectInspectors(1).asInstanceOf[StringObjectInspector]
    PrimitiveObjectInspectorFactory.javaStringObjectInspector
  }

  override def evaluate(deferredObjects: Array[GenericUDF.DeferredObject]): AnyRef = {
    val str1 = deferredObjects(0).get()
    val jsonArr = param1.getPrimitiveJavaObject(str1)

    val str2 = deferredObjects(1).get()
    val splitStr = param1.getPrimitiveJavaObject(str2)
    if (jsonArr.isEmpty || jsonArr == null) {
      ""
    } else {
      val jsonStr = jsonArr.replaceAll("\r|\n|\t", "")
      val jsonArray = JSON.parseArray(jsonStr,classOf[String])
      val scalaList = jsonArray.asScala
      scalaList.mkString(splitStr)
    }
  }

  override def getDisplayString(strings: Array[String]): String = {
    strings.mkString("------")
  }
}
