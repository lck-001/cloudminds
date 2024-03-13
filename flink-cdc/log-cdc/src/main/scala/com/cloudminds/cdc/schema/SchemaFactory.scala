package com.cloudminds.cdc.schema

import com.cloudminds.cdc.config.PropertiesFactory

import java.util

trait SchemaFactory extends Serializable {
  def createSchema:util.HashMap[String,String]
}
