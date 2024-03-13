package com.cloudminds.udf

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector}
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.IntWritable

class RangeUDF extends GenericUDF{

  override def initialize(objectInspectors: Array[ObjectInspector]): ObjectInspector = {
    if (objectInspectors.length != 2) throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 2 arguments.")
    if (objectInspectors(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory != PrimitiveCategory.INT || objectInspectors(1).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory != PrimitiveCategory.INT){
      throw new UDFArgumentException("The function arguments need int.")
    }
    ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)
  }

  override def evaluate(deferredObjects: Array[GenericUDF.DeferredObject]): AnyRef = {
    val start = deferredObjects(0).get()
    val end = deferredObjects(1).get()
    var start_index = 0
    var end_index = 0
    try{
      start_index = start.asInstanceOf[Int]
      end_index = end.asInstanceOf[Int]
    }catch {
      case e:UDFArgumentException => throw new UDFArgumentException("类型转换出错")
    }


    val arr = Array.range(start_index,end_index)
//    arr
    val arrWritable = arr.map(new IntWritable(_))
    arrWritable
  }

  override def getDisplayString(strings: Array[String]): String = {
    strings.mkString(",")
  }
}
