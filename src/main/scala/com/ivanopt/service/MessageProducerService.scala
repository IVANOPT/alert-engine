package com.ivanopt.service

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Ivan on 07/05/2018.
  */
trait MessageProducerService extends StreamingDataService {

  def publish(dStream: DStream[String], topic: String)

  def publish(rdd: RDD[String], topic: String)

}
