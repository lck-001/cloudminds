package com.cloudminds.sg.cdc.config

import org.apache.flink.core.fs.FileSystem
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream}
import java.net.URI
import java.util

object PropertiesUtils {

  def getPropertiesMap(filePath:String): util.HashMap[String, Object] ={
    val yamlPath = filePath
    //读取指定路径yaml 文件
    val stream = new FileInputStream(new File(yamlPath))
    // 只能获取当前程序相同目录文件
//    val stream = getClass.getResourceAsStream("config.yaml")
    val yaml = new Yaml()
    val prop = yaml.load(stream).asInstanceOf[util.HashMap[String, Object]]
    prop
  }

/*  def getHdfsPropertiesMap(filePath:String):util.HashMap[String,Object] = {
    var fs:FileSystem = null
    try {
      val conf = new Configuration() // 加载配置文件
      conf.set("dfs.client.use.datanode.hostname", "true")
      val uri = new URI("hdfs://nameservice1:8020") // 连接资源位置
      fs = FileSystem.get(uri, conf, "hadoop") // 创建文件系统实例对象
      val p = new Path(filePath) // 默认是读取/user/navy/下的指定文件
      val fsin = fs.open(fs.getFileStatus(p).getPath)
      val yaml = new Yaml()
      val prop = yaml.load(fsin).asInstanceOf[util.HashMap[String, Object]]
      fsin.close
      prop
    } catch {
      case e: Exception =>
        throw new Exception("未读取到配置文件! "+e.printStackTrace())
    }
  }*/
}