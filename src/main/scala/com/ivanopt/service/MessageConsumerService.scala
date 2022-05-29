package com.ivanopt.service

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Ivan on 07/05/2018.
  */

trait MessageConsumerService extends StreamingDataService {

  def subscribe(streamingContext: StreamingContext): DStream[(String, String)]

  def subscribeInMinutes(streamingContext: StreamingContext, windowDuration: Integer, slideDuration: Integer): DStream[(String, String)]

  def intermediateDataStoreServiceSelector(): IntermediateDataStoreService

  def startupExportPro(streamingContext: StreamingContext): Unit = {
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  override def process(windowDuration: Integer, slideDuration: Integer): Unit = {
    val sc: SparkContext = initSparkContext()
    val streamingContext: StreamingContext = initStreamingContext(sc)
    val currentDs: DStream[(String, String)] = if (windowDuration == 0)
      subscribe(streamingContext) else subscribeInMinutes(streamingContext, windowDuration, slideDuration)
    currentDs.count.print
    currentDs.foreachRDD(rdd => {
      intermediateDataStoreServiceSelector().storeData(rdd.map(_._2).map(record => ("5:1234567890", "calculator", "min", record)))
    })
    startupExportPro(streamingContext)
  }

}
