package com.cloudminds.cdc.service

import com.cloudminds.cdc.model.CommonSource

trait GenCommonSourceFactory extends Serializable {
  def getCommonSource(str:String): CommonSource
}
