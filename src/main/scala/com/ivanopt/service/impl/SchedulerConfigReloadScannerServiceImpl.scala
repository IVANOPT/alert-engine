package com.ivanopt.service.impl

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Timer

import com.ivanopt.scheduler.Scheduler
import com.ivanopt.service.ScannerService

import scala.collection.mutable

/**
  * Created by Ivan on 03/07/2018.
  */
class SchedulerConfigReloadScannerServiceImpl(timer: Timer) extends ScannerService[Scheduler] {

  private var notifiers: mutable.HashSet[Scheduler] = new mutable.HashSet()
  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[SchedulerConfigReloadScannerServiceImpl])

  override def add(scheduler: Scheduler): ScannerService[Scheduler] = {
    this.notifiers += scheduler
    return this
  }

  override def del(scheduler: Scheduler): ScannerService[Scheduler] = {
    this.notifiers -= scheduler
    return this
  }

  override def doNotify(): Unit = {
    log.info("SchedulerConfigReloadScannerServiceImpl.doNotify()")
    // wait for main process completed
    Thread.sleep(60 * 1000)
    while (true) {
      val currentDate: String = new SimpleDateFormat("YYYYMMddHHmmss").format(Calendar.getInstance().getTime())
      if (scanRequired(currentDate)) {
        log.info(s"Do notification on $currentDate")
        for (notifier <- notifiers) {
          notifier.activeConfigReload()
        }
        // avoid multiple scanRequired activated
        Thread.sleep(60 * 1000)
      }
    }
  }

  // scan on the hour
  def scanRequired(currentDate: String): Boolean = {
    return currentDate.endsWith("0000")
  }

  override def run(): Unit = {
    doNotify()
  }

}
