package com.cloudminds.cdc.transform

import com.alibaba.fastjson.JSON
import com.cloudminds.cdc.utils.{JsonHandler, Placeholder}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, Configuration}

import scala.collection.JavaConversions._
import java.util
import scala.util.control._

class CustomerFilterMapFunction(sourceType: String) extends RichFilterFunction[String]{
  private val kafkaSchema = new util.HashMap[String, (String, util.HashMap[String, String])]
//  val break = new Breaks;

  override def open(parameters: Configuration): Unit = {
    val globalJobParameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val globConf: Configuration = globalJobParameters.asInstanceOf[Configuration]
    val kafkaSourceCfg: ConfigOption[util.Map[String, String]] = ConfigOptions.key("kafkaSourceCfg").mapType().noDefaultValue()
    val kafkaCfgMap: util.Map[String, String] = globConf.get(kafkaSourceCfg)
    for (kv <- kafkaCfgMap ){
      val ruleSchemaArr: Array[String] = kv._2.split("###")
      val schemaMap: util.HashMap[String, String] = JSON.parseObject(ruleSchemaArr.last, classOf[util.HashMap[String, String]])
      kafkaSchema.put(kv._1.replaceAll("`",""),(ruleSchemaArr.head,schemaMap))
    }
  }
  override def filter(str: String): Boolean = {
    if ("kafka".equalsIgnoreCase(sourceType)){
      var flag = false
      // 循环遍历 从kafka中取出符合过滤的数据,仅支持json数据格式协议的kafka

      Breaks.breakable{
        for (kv <- kafkaSchema){
          val rule: String = kv._2._1
          val flatMap = new util.HashMap[String, Object]()
          JsonHandler.analysisJson(str,"",flatMap)
          // 获取匹配规则
          val dataTable: String = Placeholder.replace(rule, flatMap, "${", "}", false)
          if (kv._1.equalsIgnoreCase(dataTable)){
            flag = true
            Breaks.break()
          }
        }
      }
      flag
    }else{
      true
    }
  }
}
