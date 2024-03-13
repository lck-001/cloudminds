package com.cloudminds.cdc.model.source

import java.util

case class MqttSourceModel(dataEnv: String, hostname: String, port: Int, username: String, password: String, topic: String, qos: Int, clientId: String, cleanSession: Boolean, connectionTimeout: Int, keepAliveInterval: Int, queueSize: Int,
                           tableRule: util.HashMap[String, String], tableSchema: util.HashMap[String, util.HashMap[String, String]])
