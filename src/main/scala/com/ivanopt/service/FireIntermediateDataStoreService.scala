package com.ivanopt.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ivanopt.repository.{ConfigManagementRepository, IntermediateDataStoreSelectorRepository, RowKeyBuilderRepository}
import com.ivanopt.service.impl.{AlertRealTimeDataServiceImpl, StreamingDataConfigReloadScannerServiceImpl}
import com.ivanopt.utils.alert.{AlertSinkUtils, IntermediateDataOpUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


/**
  * Created by Ivan on 10/06/2018.
  */
trait FireIntermediateDataStoreService extends StreamingDataService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[FireIntermediateDataStoreService])

  protected var dataFunnel: DataFunnel
  protected var configManagementRepositoryTop: ConfigManagementRepository

  private val intermediateDataStoreSelectorService = new IntermediateDataStoreSelectorRepository()

  def consume(streamingContext: StreamingContext): DStream[(String, String)]

  def consume(ds: DStream[(String, String)], windowDuration: Integer, slideDuration: Integer): DStream[(String, String)]

  // duration with 1 minute for both window and slide aspects.
  override def process(windowDuration: Integer, slideDuration: Integer): Unit = {


    val sc: SparkContext = initSparkContext()
    val alertService = new AlertRealTimeDataServiceImpl().init()

    while (true) {
      log.info(s"FireIntermediateDataStoreService.process() | configReloadDemand: $configReloadDemand")

      if (configReloadDemand) {
        log.info("FireIntermediateDataStoreService.process() | Streaming context wait for startup")
        val streamingContext: StreamingContext = initStreamingContext(sc)
        var scanner: ScannerService[StreamingDataService] = new StreamingDataConfigReloadScannerServiceImpl(streamingContext)
        scanner.add(this).start()

        val dStream: DStream[(String, String)] = consume(streamingContext)
        val alertConfigRDD: RDD[String] = sc.textFile(configManagementRepositoryTop.BIZ_ALERT_CONFIG_LOCATION)
        val alertConfigs = alertConfigRDD.collect()
        for (config <- alertConfigs) {
          val configJson: JSONObject = AlertSinkUtils.statsTypeConversionForStorage(JSON.parseObject(config))
          val currentDs: DStream[(String, String)] = if (windowDuration == 0)
            dStream else consume(dStream, windowDuration, slideDuration)
          currentDs.count().print()
          val dStreamFilter: DStream[(JSONObject)] = dataFunnel
            .filterByCondition(currentDs.map(record => new JSONParser().parse(record._2)),
              IntermediateDataOpUtils.retrieveTimeRange(configJson), IntermediateDataOpUtils.retrieveTimeUnit(configJson))
          dStreamFilter.count().print()
          val result: DStream[JSONObject] = alertService.notifyOb(configJson, dStreamFilter)
          result.count().print()
          log.info("FireIntermediateDataStoreService.process() | statsType: "
            .concat(IntermediateDataOpUtils.retrieveStatsType(configJson)))
          doDataStore(IntermediateDataOpUtils.retrieveStatsType(configJson), result)
        }
        configReloadDemand = false
        startupStreamingContext(streamingContext)
        scanner.interrupt()
        log.info("FireIntermediateDataStoreService.process() | Streaming context terminated.")
      }
    }

  }

  private def doDataStore(statsType: String, result: DStream[JSONObject]): Unit = {
    result.foreachRDD(rdd => {
      intermediateDataStoreSelectorService.select(statsType).storeData(rdd.map(record => (dataParser(record, statsType))))
    })
  }

  private def dataParser(config: JSONObject, statsType: String): (String, String, String, String) = {
    val rowKeyBuilderRepository = new RowKeyBuilderRepository(config)
    return (rowKeyBuilderRepository.select(statsType), "c",
      IntermediateDataOpUtils.retrieveStatsType(config), IntermediateDataOpUtils.retrieveFinalValue(config))
  }

}
