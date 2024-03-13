package com.cloudminds.udf


import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}

import java.util


/**
 * @Author jun.luo
 * @create 2021/8/6 13:46
 */
class TTSUdtf extends GenericUDTF {

  var arr: Array[String] = new Array[String](8)

  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
    if (argOIs.length != 1) {
      throw new UDFArgumentLengthException("UserDefinedUDTF takes only one argument")
    }
    if (argOIs(0).getCategory != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("UserDefinedUDTF takes string as a parameter")
    }

    val fieldNames = new util.ArrayList[String]
    val fieldOIs = new util.ArrayList[ObjectInspector]
    fieldNames.add("tts_emoji")
    fieldNames.add("tts_payload")
    fieldNames.add("tts_action")
    fieldNames.add("tts_text")
    fieldNames.add("tts_audio")
    fieldNames.add("tts_language")
    fieldNames.add("tts_type")
    fieldNames.add("tts_step")

    //这里定义的是输出列字段类型
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  override def process(objects: Array[AnyRef]): Unit = {
    if (objects(0) == null || objects(0).toString.isEmpty){
      forward(Array("","","","","","","",""))
    }else{
      try {
        val jsonArray = JSON.parseArray(objects(0).toString,classOf[String])
        var step = 1
        val iter = jsonArray.iterator()
        while (iter.hasNext){
          val json = iter.next()
          val jsonObj = JSON.parseObject(json.toString)
          val tts_emoji = jsonObj.getOrDefault("emoji", "").toString
          val tts_payload = jsonObj.getOrDefault("payload", "").toString
          val tts_action = jsonObj.getOrDefault("action","").toString
          val tts_text = jsonObj.getOrDefault("text", "").toString
          val tts_audio = jsonObj.getOrDefault("audio", "").toString
          val tts_language = jsonObj.getOrDefault("lang", "").toString
          val tts_type = jsonObj.getOrDefault("type", "").toString
          arr(0)=tts_emoji
          arr(1)=tts_payload
          arr(2)=tts_action
          arr(3)=tts_text
          arr(4)=tts_audio
          arr(5)=tts_language
          arr(6)=tts_type
          arr(7)=step.toString
          step+=1
          forward(arr)
        }
        /*jsonArray.forEach(json=>{
          val jsonObj = JSON.parseObject(json.toString)
          val tts_emoji = jsonObj.getOrDefault("emoji", "").toString
          val tts_payload = jsonObj.getOrDefault("payload", "").toString
          val tts_action = jsonObj.getOrDefault("action","").toString
          val tts_text = jsonObj.getOrDefault("text", "").toString
          val tts_audio = jsonObj.getOrDefault("audio", "").toString
          val tts_language = jsonObj.getOrDefault("lang", "").toString
          val tts_type = jsonObj.getOrDefault("type", "").toString
          arr(0)=tts_emoji
          arr(1)=tts_payload
          arr(2)=tts_action
          arr(3)=tts_text
          arr(4)=tts_audio
          arr(5)=tts_language
          arr(6)=tts_type
          forward(arr)
        })*/
      }catch {
        case ex: Exception => {
          throw new Exception(ex+" error argument")
        }
      }
    }
  }

  override def close(): Unit = {}
}
