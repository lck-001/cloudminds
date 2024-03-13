package com.cloudminds.cdc.model.source

import java.util

case class KafkaSourceModel(dataEnv: String, bootstrapServers: String, topic: String, groupId: String, startingOffsets: String,
                            tableRule: util.HashMap[String, String], tableSchema: util.HashMap[String, util.HashMap[String, String]])
