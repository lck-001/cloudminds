package com.cloudminds.cdc.model.base

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup

case class BaseModel(jobName: String, sourceType: String, checkpointTime: Long, checkpointMode: CheckpointingMode, checkpointPause: Long, checkpointTimeout: Long, checkpointCleanup: ExternalizedCheckpointCleanup, checkpointPath: String, maxConcurrentCheckpoint: Int)
