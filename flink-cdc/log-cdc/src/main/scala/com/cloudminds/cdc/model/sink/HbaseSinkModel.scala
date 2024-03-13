package com.cloudminds.cdc.model.sink

case class HbaseSinkModel(dbTable: String, registerSql: String, sinkSql: String)
