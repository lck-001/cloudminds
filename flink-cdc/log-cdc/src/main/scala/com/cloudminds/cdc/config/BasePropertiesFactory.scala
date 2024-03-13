package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.base.BaseModel
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

import java.util

class BasePropertiesFactory(confPath: String) extends PropertiesFactory[BaseModel] {
  override def getProperties: BaseModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("base").asInstanceOf[util.HashMap[String,Object]]
    val jobName: String = Option(propMap.get("jobName")).getOrElse("log-cdc").asInstanceOf[String]
    val sourceType: String = Option(propMap.get("sourceType")).getOrElse("mysql").asInstanceOf[String]
    val checkpointMode: String = Option(propMap.get("checkpointMode")).getOrElse("exactly_once").asInstanceOf[String]
    val checkpointTime: Long = propMap.getOrDefault("checkpointTime", "60000").toString.toLong
    val checkpointPause: Long = propMap.getOrDefault("checkpointPause", "5000").toString.toLong
    val checkpointTimeout: Long = propMap.getOrDefault("checkpointTimeout", "120000").toString.toLong
    val checkpointCleanup: String = Option(propMap.get("checkpointCleanup")).getOrElse("retain_on_cancellation").asInstanceOf[String]
    val checkpointPath: String = Option(propMap.get("checkpointPath")).getOrElse("hdfs://cdh-master.cloudminds.com/tmp/flink/checkpoints/project/").asInstanceOf[String]
    val maxConcurrentCheckpoint: Int = Option(propMap.get("maxConcurrentCheckpoint")).getOrElse(1).asInstanceOf[Int]
    val mode: CheckpointingMode = checkpointMode.toLowerCase match {
      case "exactly_once" => CheckpointingMode.EXACTLY_ONCE
      case "at_least_once" => CheckpointingMode.AT_LEAST_ONCE
    }
    val cleanup: ExternalizedCheckpointCleanup = checkpointCleanup.toLowerCase match {
      case "delete_on_cancellation" => ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
      case "retain_on_cancellation" => ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
      case "no_externalized_checkpoint" => ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS
    }
    BaseModel(jobName = jobName, sourceType = sourceType, checkpointTime = checkpointTime, checkpointMode = mode, checkpointPause = checkpointPause, checkpointTimeout = checkpointTimeout, checkpointCleanup = cleanup, checkpointPath = checkpointPath, maxConcurrentCheckpoint = maxConcurrentCheckpoint)
  }
}