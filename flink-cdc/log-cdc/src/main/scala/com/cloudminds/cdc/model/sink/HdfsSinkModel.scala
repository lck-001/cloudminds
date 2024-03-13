package com.cloudminds.cdc.model.sink

case class HdfsSinkModel(dbTable: String, transformSql: String, hdfsPath: String, partitions: String)
