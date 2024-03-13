package com.cloudminds.kafka.jdbc.schema

object RowTypeTransform {
  def getDataType(dataType: String,data: Object):Object={
    dataType.toLowerCase match {
      case "string" => if (data == null) null else data.toString
      case "int" => if (data == null) null else data.toString.toInt.asInstanceOf[java.lang.Integer]
      case "float" => if (data == null) null else data.toString.toFloat.asInstanceOf[java.lang.Float]
      case "double" => if (data == null) null else data.toString.toDouble.asInstanceOf[java.lang.Double]
      case "bytes" => if (data == null) null else data.toString.toByte.asInstanceOf[java.lang.Byte]
      case "long" => if (data == null) null else data.toString.toLong.asInstanceOf[java.lang.Long]
      case "boolean" => if (data == null) null else data.toString.toBoolean.asInstanceOf[java.lang.Boolean]
      case _ => if (data == null) null else data.toString
    }
  }
}
