package com.cloudminds.cdc.config

import com.cloudminds.cdc.connector.SourceConnector.propertiesFilePath
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream}
import java.net.URI
import java.util

object PropertiesUtils {

 private def getPropertiesMap(filePath:String): util.HashMap[String, Object] ={
    val yamlPath = filePath
    //读取指定路径yaml 文件
    val stream = new FileInputStream(new File(yamlPath))
    // 只能获取当前程序相同目录文件
//    val stream = getClass.getResourceAsStream("config.yaml")
    val yaml = new Yaml()
    val prop = yaml.load(stream).asInstanceOf[util.HashMap[String, Object]]
    prop
  }

 private def getHdfsPropertiesMap(filePath:String):util.HashMap[String,Object] = {
/*    System.setProperty("java.security.krb5.conf", getPath("krb5.conf"))
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")*/
    var fs:FileSystem = null
    try {
      val conf = new Configuration() // 加载配置文件
      conf.set("dfs.client.use.datanode.hostname", "true")
/*      conf.set("hadoop.security.authentication", "kerberos")
      conf.set("kerberos.keytab", getPath("flink.keytab"))
      conf.addResource("core-site.xml")

      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("flink", getPath("flink.keytab"))*/
      val uri = new URI(filePath) // 连接资源位置
      fs = FileSystem.get(uri, conf) // 创建文件系统实例对象
//      val p = new Path(filePath) // 默认是读取/user/navy/下的指定文件
      val fsin = fs.open(new Path(filePath))
      val yaml = new Yaml()
      val prop = yaml.load(fsin).asInstanceOf[util.HashMap[String, Object]]
      fsin.close
      prop
    } catch {
      case e: Exception =>
        throw new Exception("未读取到配置文件! "+e.printStackTrace())
    }
  }
  def getProperties(conf_path:String):util.HashMap[String,Object]={
    var prop:util.HashMap[String,Object] = null
    if ("hdfs".equalsIgnoreCase(conf_path.split("://").head)){
      prop = PropertiesUtils.getHdfsPropertiesMap(conf_path)
    }else{
      prop = PropertiesUtils.getPropertiesMap(conf_path)
    }
    prop
  }
  def main(args: Array[String]): Unit = {
    getHdfsPropertiesMap("hdfs://nameservice1/tmp/flink/c0004_t_login_seats.yaml")
  }
  def getPath(filename:String):String={
    if (null == filename) return null
    Thread.currentThread.getContextClassLoader.getResource(filename).getPath
  }
}