package com.ivanopt.service

import org.apache.spark.streaming.StreamingContext

/**
 * Created by Ivan on 21/06/2018.
 */
trait ScannerService[T] extends Thread {

  def add(task: T): ScannerService[T]

  def del(task: T): ScannerService[T]

  def doNotify()

}