package com.ivanopt.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.impl.StreamingDataConfigReloadScannerServiceImpl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Ivan on 08/06/2018.
  */
trait FireAlertCalculatorService extends StreamingDataService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[FireAlertCalculatorService])

  protected var dataFunnel: DataFunnel
  protected var configManagementRepositoryTop: ConfigManagementRepository
  private val unitMappingMinutes: Map[String, java.lang.Integer] = Map("min" -> 1, "hour" -> 60, "day" -> 24 * 60)

  //protected val sc = initSparkContext()

  // Currently use source from spark engine.
  def consume(streamingContext: StreamingContext): DStream[(String, String)]

  // Currently use source from spark engine.
  def consume(ds: DStream[(String, String)], windowDuration: Integer, slideDuration: Integer): DStream[(String, String)]

  def doFilter(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject]

  def fireAlert(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject]

  def alertCollector(ds: DStream[JSONObject])

  def process(): Unit = {

    val sc: SparkContext = initSparkContext()
    var engineToStartSignal = true

    while (true) {
      if (engineToStartSignal) {
        log.info(s"FireAlertCalculatorService.process() | Streaming context wait for startup")
        val streamingContext: StreamingContext = initStreamingContext(sc)
        var scanner: ScannerService[StreamingDataService] =
          new StreamingDataConfigReloadScannerServiceImpl(streamingContext)
        scanner.add(this).start()

        //TODO: filter real and intermediate data according to time unit
        val dStream: DStream[(String, String)] = consume(streamingContext)
        val alertConfigRDD: RDD[String] = sc.textFile(configManagementRepositoryTop.BIZ_ALERT_CONFIG_LOCATION)
        val alertConfigs = alertConfigRDD.collect()
        for (config <- alertConfigs) {
          val configJson = JSON.parseObject(config)
          val timeRange: java.lang.Integer = configJson.get("timeNum").asInstanceOf[java.lang.Integer]
          val timeUnit: String = configJson.get("timeUnit").asInstanceOf[String]
          // val windowDuration = calculateWindowDuration(timeRange, timeUnit)
          // the result windowDuration should less than 360 min
          val windowDuration = 1
          // TODO: don window duration retriever and do consumer switcher.
          val currentDs: DStream[(String, String)] = if (windowDuration == 0)
            dStream else consume(dStream, windowDuration, 1)
          val dStreamFilter: DStream[(JSONObject)] = dataFunnel
            .filterByCondition(currentDs.map(record => new JSONParser().parse(record._2)), timeRange, timeUnit)
          dStreamFilter.count().print()
          val filterDs: DStream[JSONObject] = doFilter(configJson, dStreamFilter)
          filterDs.count().print()
          val alertDs = fireAlert(configJson, filterDs)
          if (alertDs != null) {
            alertDs.count().print()
            alertCollector(alertDs)
          }
        }
        engineToStartSignal = false
        startupStreamingContext(streamingContext)
        scanner.interrupt()
        log.info("FireAlertCalculatorService.process() | Streaming context terminated.")
      }
    }
  }

  private def calculateWindowDuration(timeRange: java.lang.Integer, timeUnit: String): java.lang.Integer = {
    return timeRange * unitMappingMinutes(timeUnit)
  }

}
