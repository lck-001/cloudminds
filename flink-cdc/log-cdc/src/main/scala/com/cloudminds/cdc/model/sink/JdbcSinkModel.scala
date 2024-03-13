package com.cloudminds.cdc.model.sink

case class JdbcSinkModel(dbTable: String, jdbcType: String, transformSql: String, jdbcUrl:String,jdbcDriver: String, username: String, password: String,
                         batchSize: Int, maxRetries: Int, batchInterval: Int, sinkDatabase: String, sinkTable: String)
