package com.ivanopt.trigger

import com.ivanopt.service.StreamingDataService

trait Trigger {

  protected def streamingDataSelector(): StreamingDataService

  def process(): Unit = {
    streamingDataSelector().process(1, 1)
  }


}