package com.ivanopt.service.impl

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ivanopt.service.{ScannerService, StreamingDataService}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

/**
  * Created by Ivan on 21/06/2018.
  */
class StreamingDataConfigReloadScannerServiceImpl(streamingContext: StreamingContext)
  extends ScannerService[StreamingDataService] {

  private var notifiers: mutable.HashSet[StreamingDataService] = new mutable.HashSet()
  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[StreamingDataConfigReloadScannerServiceImpl])

  override def add(streamingDataService: StreamingDataService): ScannerService[StreamingDataService] = {
    this.notifiers += streamingDataService
    return this
  }

  override def del(streamingDataService: StreamingDataService): ScannerService[StreamingDataService] = {
    this.notifiers -= streamingDataService
    return this
  }

  override def doNotify(): Unit = {
    // wait for main process completed
    Thread.sleep(60 * 1000)
    while (true) {
      val currentDate: String = new SimpleDateFormat("YYYYMMddHHmmss").format(Calendar.getInstance().getTime())
      if (scanRequired(currentDate)) {
        log.info(s"Do notification on $currentDate")
        for (notifier <- notifiers) {
          notifier.stopStreamingContext(streamingContext)
        }
      }
    }
  }

  // scan required on the hour
  def scanRequired(currentDate: String): Boolean = {
    return currentDate.endsWith("0000")
  }

  override def run(): Unit = {
    doNotify()
  }

}
