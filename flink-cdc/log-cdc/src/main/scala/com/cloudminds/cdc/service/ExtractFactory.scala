package com.cloudminds.cdc.service

import com.cloudminds.cdc.model.CommonSource
import org.apache.avro.generic.GenericRecord

trait ExtractFactory extends Serializable {
  def getGenericRecord(common:CommonSource): GenericRecord
}
