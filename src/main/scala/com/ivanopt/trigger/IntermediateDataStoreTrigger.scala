package com.ivanopt.trigger

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.StreamingDataService
import com.ivanopt.service.impl.IntermediateDataStoreViaHBaseServiceImpl

object IntermediateDataStoreTrigger extends Trigger {

  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository()

  override protected def streamingDataSelector(): StreamingDataService = {
    return new IntermediateDataStoreViaHBaseServiceImpl(configManagementRepository)
  }

  def main(args: Array[String]): Unit = {
    process()
  }

}
