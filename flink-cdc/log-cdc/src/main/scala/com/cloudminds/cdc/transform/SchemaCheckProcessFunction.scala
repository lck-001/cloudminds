package com.cloudminds.cdc.transform

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.model.CommonSource
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.api.scala._

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


class SchemaCheckProcessFunction(changeSchemaTag : OutputTag[JSONObject]) extends KeyedProcessFunction[String,CommonSource,CommonSource]{
  lazy val schema: ValueState[util.HashMap[String, String]] = getRuntimeContext.getState(new ValueStateDescriptor[util.HashMap[String, String]]("schema", classOf[util.HashMap[String, String]]))
  private val queryableDescriptor = new MapStateDescriptor[String,String]("queryable", classOf[String], classOf[String])
  queryableDescriptor.setQueryable("query-name")
  lazy val queryableState: MapState[String, String] = getRuntimeContext.getMapState(queryableDescriptor)
  override def processElement(common: CommonSource, context: KeyedProcessFunction[String, CommonSource, CommonSource]#Context, collector: Collector[CommonSource]): Unit = {
    queryableState.put(common.dbTable,common.data)
    collector.collect(common)
    // 如果源数据是包含定义schema的,提取到的schema不为空
    if (null != common && common.schema.nonEmpty){
      // 如果初始schema为空,把当前数据的schema填充进去,否则拿出状态中的schema信息对比
      if(null == schema.value()){
        schema.update(common.schema)
      }else{
        // 拿出状态的schema和当前的数据的schema对比，如果不相等 输出当前数据和之前schema 和 当前schema信息
        if (!common.schema.equals(schema.value())){
          // 先缓存之前的schema
          val beforeSchema: util.HashMap[String, String] = schema.value()
          // TODO 更新状态中的schema
          schema.update(common.schema)
          val json = new JSONObject()
          json.put("dbTable",common.dbTable)
          json.put("bigdata_method",common.bigdata_method)
          json.put("event_time",common.event_time)
          json.put("k8s_env_name",common.dataEnv)
          json.put("data",common.data)
          json.put("beforeSchema",beforeSchema)
          json.put("afterSchema",common.schema)
          context.output(changeSchemaTag,json)
        }
      }
    }
  }
}

/*
class SchemaCheckProcessFunction(changeSchemaTag : OutputTag[JSONObject]) extends KeyedProcessFunction[String,CommonSource,JSONObject] {


  lazy val schema: ValueState[util.HashMap[String, String]] = getRuntimeContext.getState(new ValueStateDescriptor[util.HashMap[String, String]]("schema", classOf[util.HashMap[String, String]]))

  override def processElement(cs: CommonSource, ctx: KeyedProcessFunction[(String, String), CommonSource, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    // 先输出所有进来的数据

    val js = new JSONObject()
    js.put("bigdata_method",cs.bigdata_method)
    js.put("schema",cs.schema)
    js.put("event_time",cs.event_time)
    js.put("before",cs.before)
    js.put("after",cs.after)

    out.collect(js)
    // 先从value state中取数据，如果没有就把当前的schema信息存进去,否则取出后对比,没变化无需关注,否则输出变化前后的schema 以及表信息
    if (schema.value() == null){
      schema.update(cs.schema)
    }else{
      if (!cs.schema.equals(schema.value())){
        val beforeSchema = schema.value()
        schema.update(cs.schema)
        val json = new JSONObject()
        json.put("db",cs.db)
        json.put("table",cs.table)
        json.put("event_time",cs.event_time)
        json.put("bigdata_method",cs.bigdata_method)
        json.put("before",cs.before)
        json.put("after",cs.after)
        json.put("beforeSchema",beforeSchema)
        json.put("afterSchema",cs.schema)
        ctx.output(changeSchemaTag,json)
      }
    }
  }
}*/
