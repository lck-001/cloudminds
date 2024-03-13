package com.cloudminds.cdc.model

case class CheckpointModel(checkpointTime:Long,checkpointPause:Long,checkpointTimeout:Long,checkpointPath:String,maxConcurrentCheckpoint:Int)
