package com.cloudminds.udf

import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentTypeException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}

import java.util
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class DimUDF extends GenericUDF {

  private var param1:StringObjectInspector = _
  private var param2:ListObjectInspector = _
  val dimMap = new util.HashMap[String,String]
  val timeList = new ArrayBuffer[String]
  override def initialize(objectInspectors: Array[ObjectInspector]): ObjectInspector = {
    if (objectInspectors.length != 2) throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 2 arguments.")
    if (objectInspectors(0).asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory != PrimitiveCategory.STRING){
      throw new UDFArgumentException("The function first argument need string.")
    }
    if (!(objectInspectors(1).getCategory.equals(ObjectInspector.Category.LIST))){
      throw new UDFArgumentTypeException(1, "\"array\" expected at function ARRAY_CONTAINS, but \""
        + objectInspectors(1).getTypeName + "\" " + "is found");
    }
    param1 = objectInspectors(0).asInstanceOf[StringObjectInspector]
    param2 = objectInspectors(1).asInstanceOf[ListObjectInspector]

    PrimitiveObjectInspectorFactory.javaStringObjectInspector

  }

  override def evaluate(deferredObjects: Array[GenericUDF.DeferredObject]): AnyRef = {
    // 对数组先切分, 切分后按照end_time升序排序,使用二分法查找
    val arrObj = deferredObjects(1).get()
    val arr = param2.getList(arrObj)
    val endTimeObj = deferredObjects(0).get()
    val endTime = param1.getPrimitiveJavaObject(endTimeObj)
    if (arr.isEmpty || ( arr.size() == 1 && arr.get(0).toString.isEmpty) || endTime.isEmpty){
       ""
    } else {
      val iter = arr.iterator()
      while (iter.hasNext){
        val value = iter.next()
        if (value != null){
          val endTimeKey = value.toString.split("->")(1)
          val dimValue = value.toString.split("->")(2)
          dimMap.put(endTimeKey,dimValue)
          timeList.append(endTimeKey)
        }
      }
      val preEndTime = search(timeList, endTime)
      val dim = dimMap.getOrDefault(preEndTime,"")
//      var dimMapStr=dimMap.toString
//      var timeListStr=timeList.toString
      dimMap.clear()
      timeList.clear()
      dim
//      "end time = "+endTime+" dim = "+dim+" map = "+dimMapStr+" list= "+timeListStr
    }
  }

  //二分法查找，返回
  def search(arr: ArrayBuffer[String], target: String): String = {
    val arrSort = arr.sortBy(x => x)
    var lf = 0
    var rt = arrSort.length - 1
    while ( {
      lf <= rt
    }) {
      val mid = lf + (rt - lf) / 2
      if (arrSort(mid).compareToIgnoreCase(target) > 0) rt = mid - 1
      else lf = mid + 1
    }
    if (lf == 0){
      target
    }else{
      arrSort(lf-1)
    }
  }

  override def getDisplayString(strings: Array[String]): String = {
    strings.mkString("------")
  }
}
