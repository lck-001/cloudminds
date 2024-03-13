package com.cloudminds.cdc.bucket

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.model.{JsonParquet, Project}
import com.cloudminds.cdc.utils.TimeUtils
import org.apache.avro.generic.GenericRecord
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

import java.io.File
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class CustomerBucketAssigner(partitions: String) extends BucketAssigner[GenericRecord,String]{
  override def getBucketId(data: GenericRecord, context: BucketAssigner.Context): String = {
    // 如果指定了分区字段 按照分区字段分区,否则默认当前系统时间
    val realPath = new ArrayBuffer[String]()
    if (null != partitions && partitions.nonEmpty){
      val columns = partitions.split(",")
      val partition = getColumnPath(columns, data)
      realPath.append(partition)
    }else{
      val ts = data.get("event_time").asInstanceOf[Long]
      val dt = getDt(ts)
      realPath.append("dt="+dt)
    }
    val finalPath = realPath.filter(path => path.nonEmpty)
    finalPath.mkString(File.separator)
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    SimpleVersionedStringSerializer.INSTANCE
  }

  def getDt(ts:Long):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(sdf.format(ts))
    sdf.format(date)
  }

  def getColumnPath(columns: Array[String], record: GenericRecord): String ={
    val partitionList = new ListBuffer[String]
    for (column <- columns){
      val col = column.trim.split("\\s")
      val columnValue = Option(record.get(col(0))).getOrElse("").toString
      // 判断是否是时间字段
      if (columnValue.length >= 10){
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val date = TimeUtils.parseDate(columnValue.substring(0,10))
        val dt = sdf.format(date)
        if (dt != null){
          val day = dt.substring(0, 10)
          partitionList.append(if (col.length>=3 && col(2).nonEmpty) col(2)+"="+day else col(0)+"="+day)
        }else{
          partitionList.append(if (col.length>=3 && col(2).nonEmpty) col(2)+"="+columnValue else col(0)+"="+columnValue)
        }
      }else{
        partitionList.append(if (col.length>=3 && col(2).nonEmpty) col(2)+"="+columnValue else col(0)+"="+columnValue)
      }
    }
    partitionList.mkString(File.separator)
  }

}
