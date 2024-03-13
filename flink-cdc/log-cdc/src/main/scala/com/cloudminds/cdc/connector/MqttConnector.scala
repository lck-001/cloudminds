package com.cloudminds.cdc.connector

import com.cloudminds.cdc.model.source.MqttSourceModel
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttCallback, MqttClient, MqttConnectOptions, MqttMessage, MqttTopic}
import org.slf4j.{Logger, LoggerFactory}

import java.io.ObjectOutputStream
import java.util.concurrent.{ArrayBlockingQueue, Executors, ScheduledExecutorService}
import scala.util.control.Breaks
import scala.util.control.Breaks.break

class MqttConnector(model:MqttSourceModel) extends RichSourceFunction[String]{
  val logger: Logger = LoggerFactory.getLogger(classOf[MqttConnector])
  var mqttClient:MqttClient = _
  var mqttTopic:MqttTopic = _
  private val queue = new ArrayBlockingQueue[String](model.queueSize)

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    connect()
    //利用死循环使得程序一直监控主题是否有新消息
    while (true){
      //使用阻塞队列的好处是队列空的时候程序会一直阻塞到这里不会浪费CPU资源
      sourceContext.collect(queue.take())
    }
  }

  override def cancel(): Unit = {

  }

  def connect(): Unit ={
    val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(10)
    val memoryPersistence = new MemoryPersistence()
    val mqttConnectOptions = new MqttConnectOptions()
    val mqttUrl: String = "tcp://"+model.hostname+":"+model.port
    val mqttClient = new MqttClient(mqttUrl, model.clientId, memoryPersistence)
    mqttConnectOptions.setUserName(model.username)
    mqttConnectOptions.setPassword(model.password.toCharArray)
    mqttConnectOptions.setConnectionTimeout(model.connectionTimeout)
    mqttConnectOptions.setKeepAliveInterval(model.keepAliveInterval)
    mqttConnectOptions.setCleanSession(model.cleanSession)

    try {
      mqttClient.setCallback(new MsgCallback(mqttClient = mqttClient,mqttConnectOptions = mqttConnectOptions,model.topic,model.qos))
      mqttClient.connect(mqttConnectOptions)
      mqttClient.subscribe(model.topic,model.qos)
      logger.info("MQTT连接成功:" + "ClientId:" + mqttClient)
    }catch {
      case e:Exception => logger.error("MQTT连接异常：" + e)
    }

  }
  class MsgCallback(mqttClient:MqttClient,mqttConnectOptions:MqttConnectOptions,topic: String,qos:Int) extends MqttCallback {
/*    var mqttClient:MqttClient = _
    var mqttConnectOptions:MqttConnectOptions = _
//    val topic:Array[String]
    var topic: String = _
    var qos:Int = 2*/
    override def connectionLost(throwable: Throwable): Unit = {
      logger.error("MQTT连接断开，发起重连")
      Breaks.breakable{
        while (true){
          try {
            Thread.sleep(1000)
            mqttClient.connect(mqttConnectOptions)
            mqttClient.subscribe(topic,qos)
            logger.info("MQTT重新连接成功:" + mqttClient)
            break
          }catch {
            case e:Exception => logger.error(e.printStackTrace().toString)
          }
        }
      }
    }

    override def messageArrived(s: String, mqttMessage: MqttMessage): Unit = {
      val msg = new String(mqttMessage.getPayload)
//      val bymsg: Array[Byte] = getBytesFromObject(msg)
      queue.put(msg)
    }

    def getBytesFromObject(obj:String):Array[Byte]={
      if (obj == null) {
        return null
      }
      val bo = new ByteArrayOutputStream()
      val oo = new ObjectOutputStream(bo)
      oo.writeObject(obj)
      bo.toByteArray
    }

    override def deliveryComplete(iMqttDeliveryToken: IMqttDeliveryToken): Unit = {

    }
  }
}
