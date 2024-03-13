package com.cloudminds.cdc.model

case class Project (id:Int,created:String,updated:String,status:Int,name:String,priority:String,import_date:String,
                    start_date:String,end_date:String,category:String,industry:String,trainer:Int,trainer_name:String,operator:Int,
                    operator_name:String,content_operator:Int,content_operator_name:String,agents:String,ts_ms:Long,op:String,db:String,table:String)
