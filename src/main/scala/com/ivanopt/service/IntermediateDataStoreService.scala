package com.ivanopt.service

import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 08/06/2018.
  */
trait IntermediateDataStoreService extends StreamingDataService {

  def storeData(toStoreRDD: RDD[(String, String, String, Any)])

  override def process(windowDuration: Integer, slideDuration: Integer): Unit = {
    val recordRDD: RDD[(String, String, String, Any)] = initSparkContext().parallelize(Seq(("3:1234567890", "calculator", "min", "alert")))
    storeData(recordRDD)
  }

}