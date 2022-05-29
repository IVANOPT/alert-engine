package com.ivanopt.trigger

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.StreamingDataService
import com.ivanopt.service.impl.FireAlertCalculatorServiceImpl

/**
 * Created by Ivan on 08/06/2018.
 */
object FireCalculatorTrigger extends Trigger {

  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository()

  override protected def streamingDataSelector(): StreamingDataService = {
    return new FireAlertCalculatorServiceImpl(configManagementRepository)
  }

  def main(args: Array[String]): Unit = {
    process()
  }

}
