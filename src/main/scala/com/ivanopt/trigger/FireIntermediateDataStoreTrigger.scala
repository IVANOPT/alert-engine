package com.ivanopt.trigger

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.StreamingDataService
import com.ivanopt.service.impl.FireIntermediateDataStoreServiceImpl

/**
  * Created by Ivan on 10/06/2018.
  */
object FireIntermediateDataStoreTrigger extends Trigger {

  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository()

  override protected def streamingDataSelector(): StreamingDataService = {
    return new FireIntermediateDataStoreServiceImpl(configManagementRepository)
  }

  def main(args: Array[String]): Unit = {
    process()
  }

}
