package com.ivanopt.trigger

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.trigger.thread.StreamDataProducerThread

/**
 * 向topic:streaming-data-tool中写入测试数据
 * 消费topic:streaming-data-tool中数据，进行过滤和聚合，
 * 计算结果落入topic:test中
 *
 * Created with IDEA
 * User: zzzz76
 * Date: 2018-06-22
 */
object FireAlertTriggerTest {
  val configManagementRepository = new ConfigManagementRepository

  def main(args: Array[String]): Unit = {
    val producerThread = new StreamDataProducerThread(configManagementRepository, "data/dsg_test_data.txt")
    producerThread.start()
    FireCalculatorTrigger.main(null)
  }
}
