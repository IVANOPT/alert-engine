package com.ivanopt.trigger

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.StreamingDataService
import com.ivanopt.service.impl.MessageProducerViaKafkaServiceImpl

/**
  * Created by Ivan on 07/06/2018.
  */
object MessageProducerTrigger extends Trigger {

  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository()

  override protected def streamingDataSelector(): StreamingDataService = {
    return new MessageProducerViaKafkaServiceImpl(configManagementRepository)
  }

  def main(args: Array[String]): Unit = {
    process()
  }

}