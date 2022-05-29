package com.ivanopt.scheduler

import java.util.Timer

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cong.yu on 18-6-22.
  */
trait Scheduler {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[Scheduler])
  protected var configReloadDemand: Boolean = true

  def activeConfigReload(): Unit = {
    log.info("Scheduler.stopTimer() | active config reload")
    this.configReloadDemand = true
  }


  def stopTimer(timer: Timer): Unit = {
    log.info("Scheduler.stopTimer() | stop timer")
    timer.cancel()
    timer.purge()
  }

  def initSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.setAppName("ivanopt-alert-service:intermediate-calculator")
    new SparkContext(conf)
  }

}
