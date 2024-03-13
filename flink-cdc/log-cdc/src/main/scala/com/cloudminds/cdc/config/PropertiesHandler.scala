package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.CheckpointModel

import java.util

object PropertiesHandler {
  /**
   * yaml 配置文件参数解析
   */
  var prop:util.HashMap[String,Object] = null
  def getYamlProp(conf_path:String):Unit={
    assert(conf_path != null || conf_path.nonEmpty,"no prop file define in current path !")
    if ("hdfs".equalsIgnoreCase(conf_path.split("://").head)){
//      prop = PropertiesUtils.getHdfsPropertiesMap(conf_path)
    }else{
//      prop = PropertiesUtils.getPropertiesMap(conf_path)
    }
  }

  /**
   * 获取 checkpoint 相关的配置
   */
  def getCheckPointConfig():CheckpointModel={
    val checkpointTime: Long = prop.getOrDefault("checkpointTime","60000").toString.toLong
    val checkpointPause: Long = prop.getOrDefault("checkpointPause","5000").toString.toLong
    val checkpointTimeout: Long = prop.getOrDefault("checkpointTimeout","120000").toString.toLong
    val checkpointPath: String = Option(prop.get("checkpointPath")).getOrElse("hdfs://cdh-master.cloudminds.com/tmp/flink/checkpoints/project/").asInstanceOf[String]
    val maxConcurrentCheckpoint: Int = Option(prop.get("maxConcurrentCheckpoint")).getOrElse(1).asInstanceOf[Int]
    CheckpointModel(checkpointTime = checkpointTime,checkpointPause = checkpointPause,checkpointTimeout = checkpointTimeout,checkpointPath = checkpointPath,maxConcurrentCheckpoint = maxConcurrentCheckpoint)
  }

}
