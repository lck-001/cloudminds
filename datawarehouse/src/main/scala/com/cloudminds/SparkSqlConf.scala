package com.cloudminds

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.net.URI

/**
 * @Author jun.luo
 * @create 2021/7/20 10:45
 */
class SparkSqlConf (app_name: String, dev_or_pro: String = "pro"){
//  System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf")
//  System.setProperty("sun.security.krb5.debug", "true")
//  System.setProperty("HADOOP_USER_NAME", "hive")
  val hdfs_conf = new Configuration()
//  hdfs_conf.set("fs.defaultFS","hdfs://nameservice-ha")
//  hdfs_conf.set("hadoop.security.authentication","kerberos")

//  hdfs_conf.set("spark.yarn.keytab","datasource.keytab")
//  hdfs_conf.set("spark.yarn.principal","datasource@CLOUDMINDS.COM")
//  hdfs_conf.set("dfs.namenode.kerberos.principal.pattern", "*/*@CLOUDMINDS.COM")

//  UserGroupInformation.setConfiguration(hdfs_conf)
//  UserGroupInformation.loginUserFromKeytab("datasource@CLOUDMINDS.COM", "src/main/resources/datasource.keytab")
  //初始化sparksession builder
  val conf: SparkSession.Builder = SparkSession.builder().enableHiveSupport().appName(app_name)//.master("local")

  //配置环境
  conf.config("hive.exec.dynamic.partition.mode", "nonstrict")
  conf.config("hive.merge.mapfiles", "true")
  conf.config("hive.merge.mapredfiles", "true")
  conf.config("hive.exec.max.dynamic.partitions", "60000")
  conf.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.config("spark.rdd.compress", "true")
  conf.config("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
  conf.config("spark.sql.crossJoin.enabled","true")
//  conf.config("spark.yarn.keytab","datasource.keytab")
//  conf.config("spark.yarn.principal","datasource@CLOUDMINDS.COM")
//  conf.config("hadoop.security.authentication","kerberos")
//  conf.config("dfs.namenode.kerberos.principal.pattern", "*/*@CLOUDMINDS.COM")



  //生成sparksession入口 spark
  val spark: SparkSession = conf.getOrCreate()
  val sc: SparkContext = spark.sparkContext

  //hdfs相关设置
  //kerberos集群配置文件配置

//  sc.hadoopConfiguration.set("hadoop.security.authentication","kerberos")
//  sc.hadoopConfiguration.set("dfs.namenode.kerberos.principal.pattern", "*/*@CLOUDMINDS.COM")
  sc.hadoopConfiguration.addResource("core-site.xml")
  sc.hadoopConfiguration.addResource("hdfs-site.xml")
  sc.hadoopConfiguration.addResource("hive-site.xml")

  //用户登录
//  UserGroupInformation.setConfiguration(sc.hadoopConfiguration)




  val hdfs_url: URI = if (dev_or_pro == "pro") URI.create("hdfs://nameservice1") else URI.create("hdfs://xxxx")
//  val hdfs_url: URI = if (dev_or_pro == "pro") URI.create("hdfs://nameservice-ha") else URI.create("hdfs://xxxx")
  val file_sys: FileSystem = FileSystem.get(hdfs_url, hdfs_conf)
}
object SparkSqlConf {
  //hbase相关设置
  val hconf: Configuration = HBaseConfiguration.create()
//  hconf.set("hbase.zookeeper.property.clientPort", "2181")
//  hconf.set("hbase.zookeeper.quorum", "xxxxx:2181,xxxx:2181,xxx:2181,xxxx:2181")
//  hconf.set("hbase.master", "xxxx")
//  hconf.set("zookeeper.znode.parent", "/hbase")
//  hconf.set("hbase.client.keyvalue.maxsize", "-1")
}
