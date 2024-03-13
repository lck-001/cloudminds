package com.cloudminds.cdc.transform

import com.cloudminds.cdc.model.CommonSource
import org.apache.flink.api.java.functions.KeySelector

class CommonKeySelector extends KeySelector[CommonSource,String]{
  override def getKey(in: CommonSource): String = {
    in.dbTable
  }
}
