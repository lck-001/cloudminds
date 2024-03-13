package com.cloudminds.cdc.model

import java.util
import scala.collection.mutable.ListBuffer

case class JdbcConfig(sourceType:String,databases:ListBuffer[String], schemas:ListBuffer[String],
                      tables:ListBuffer[String], hostname:String, port:Int, username:String,
                      password:String, slotName:String, dbTab:ListBuffer[String],
                      startupOptions:util.HashMap[String,String])
