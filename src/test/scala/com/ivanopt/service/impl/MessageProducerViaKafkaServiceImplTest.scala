package com.ivanopt.service.impl

import com.ivanopt.repository.ConfigManagementRepository
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.testng.annotations.Test

/**
 * Created with IDEA
 * User: zzzz76
 * Date: 2018-06-19
 */
class MessageProducerViaKafkaServiceImplTest {
  val configManagementRepository = new ConfigManagementRepository()
  val messageProducerViaKafkaServiceImpl = new MessageProducerViaKafkaServiceImpl(configManagementRepository)

  //测试前修改本地配置
  @Test(groups = Array("intermediate-data-store"))
  def send(): Unit = {
    var messageNo = 1
    while (true) {
      val messageStr = "Message_" + messageNo
      messageNo += 1
      println("Send: " + messageStr)
      messageProducerViaKafkaServiceImpl.send(configManagementRepository.SINK_TOPIC, messageStr, messageStr)
      try {
        Thread.sleep(5000)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }
  }

  @Test(groups = Array("intermediate-data-store"))
  def publish(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    //Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    messageProducerViaKafkaServiceImpl.publish(lines, configManagementRepository.SINK_TOPIC)
    ssc.start()
    ssc.awaitTermination()
  }
}
