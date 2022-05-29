package com.ivanopt.trigger

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.StreamingDataService
import com.ivanopt.service.impl.MessageConsumerViaKafkaServiceImpl

object MessageConsumerTrigger extends Trigger {

  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository()

  override protected def streamingDataSelector(): StreamingDataService = {
    return new MessageConsumerViaKafkaServiceImpl(configManagementRepository)
  }

  def main(args: Array[String]): Unit = {
    process()
  }

}