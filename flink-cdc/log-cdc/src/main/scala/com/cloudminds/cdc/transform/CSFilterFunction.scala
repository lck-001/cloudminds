package com.cloudminds.cdc.transform

import com.cloudminds.cdc.model.CommonSource
import org.apache.flink.api.common.functions.RichFilterFunction

class CSFilterFunction extends RichFilterFunction[CommonSource]{
  override def filter(t: CommonSource): Boolean = {
    if (null != t.dbTable && t.dbTable.nonEmpty){
      true
    }else{
      false
    }
  }
}
