package com.cloudminds.cdc.service.mqtt

import com.cloudminds.cdc.model.{CommonProp, CommonSource}
import com.cloudminds.cdc.service.GenCommonSourceFactory
import com.cloudminds.cdc.utils.{JsonHandler, Placeholder}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConversions._
import scala.util.control.Breaks

class MqttGenCommonSourceFactory (commonProp: CommonProp) extends GenCommonSourceFactory {
  val logger: Logger = LoggerFactory.getLogger(classOf[MqttGenCommonSourceFactory])
  val tableRule: util.HashMap[String, String] = commonProp.tableRule
  override def getCommonSource(str: String): CommonSource = {
    try {
      // 判断是否是json,如果是 json 按照json方式处理封装成common source 否则
      val b: Boolean = JsonHandler.isJson(str)
      val commonSource: CommonSource = CommonSource("", commonProp.dataEnv,"", 0L,new util.HashMap[String, String](), str)
      if (b) {
        val placeMap = new util.HashMap[String, Object]()
        // 转换成map数据
        JsonHandler.analysisJson(str, "", placeMap)
        assert(null != tableRule && tableRule.nonEmpty, "no mqtt rule define, please set mqtt rule ")
        Breaks.breakable {
          for (rule <- tableRule) {
            val tmpValue: String = Placeholder.replace(rule._2, placeMap, "${", "}", false)
            // 如果根据定义规则匹配到符合条件的数据输出common source, 终止当前循环
            if (tmpValue.equalsIgnoreCase(rule._1)) {
              commonSource.dbTable = rule._1
              commonSource.bigdata_method = "c"
              Breaks.break()
            }
          }
        }
      }
      commonSource
    }catch {
      case e: Exception => {
        logger.error("json format failed, please check str value or customer parse str " + e)
        CommonSource("", "", "", 0L,new util.HashMap[String, String](), str)
      }
    }
  }
}
