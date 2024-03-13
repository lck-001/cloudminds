package com.cloudminds.cdc.service

import com.cloudminds.cdc.config.PropertiesFactory
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util

trait SourceFactory extends Serializable {

  def createSource(env: StreamExecutionEnvironment): DataStreamSource[String]
}
