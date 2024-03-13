package com.cloudminds.cdc.service

trait TransformFactory {
  def transformTable(): Unit
}
