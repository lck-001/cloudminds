package com.cloudminds.cdc.service

trait SinkFactory {
  def executeSink(): Unit
}
