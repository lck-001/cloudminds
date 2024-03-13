package com.cloudminds.cdc.config


trait PropertiesFactory[T] extends Serializable {
  def getProperties:T
}