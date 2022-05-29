package com.ivanopt.service.impl

import com.alibaba.fastjson.JSONObject
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service._
import com.ivanopt.utils.alert.{AlertSinkUtils, AlertUtils}
import com.ivanopt.utils.aviator.AviatorFunction
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ivan on 08/06/2018.
  */
class FireAlertCalculatorServiceImpl(configManagementRepository: ConfigManagementRepository) extends FireAlertCalculatorService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[FireAlertCalculatorServiceImpl])

  private final val alertService = new AlertRealTimeDataServiceImpl().init()
  private final val messageProducerViaKafkaServiceImpl = new MessageProducerViaKafkaServiceImpl(configManagementRepository)

  override def doFilter(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    val filterInfo: String = rule.getString("filterInfo")
    if (StringUtils.isBlank(filterInfo)) {
      return ds
    }

    try {
      ds.filter(AviatorFunction.execute(AviatorFunction.conversion(filterInfo), _))
    } catch {
      case e: Exception => {
        log.error("Trace FireAlertCalculatorServiceImpl.doFilter() |" + e.getMessage, e)
        return ds
      }
    }
  }

  override def fireAlert(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    val calculateResult = alertService.notifyOb(rule, ds)
    if (calculateResult == null) {
      log.error(s"fireAlert failed with null pointer calculate result, rule details $rule")
      return null
    }
    return calculateResult.filter(line => AlertUtils.execute(line, rule))
  }

  override def alertCollector(alertDs: DStream[JSONObject]): Unit = {
    val storeDs = alertDs.map(AlertSinkUtils.convertCollectorSource)
    messageProducerViaKafkaServiceImpl.publish(storeDs, configManagementRepository.SINK_TOPIC)
  }

  override def consume(streamingContext: StreamingContext): DStream[(String, String)] = {

    val topicList = configManagementRepository.SOURCE_TOPIC
    val brokerList = configManagementRepository.BROKER_LIST
    val groupId = configManagementRepository.GROUP_ID
    val zkList = configManagementRepository.ZK_LIST
    val kafkaParams: Map[String, String] = Map("bootstrap.servers" -> brokerList, "group.id" -> groupId)
    val topics = Set(topicList)

    val offsetsStoreService: OffsetsStoreService = new ZookeeperOffsetStoreServiceImpl(topicList, zkList, "calculator")
    val storedOffsets = offsetsStoreService.readOffsets()

    val kafkaStream = storedOffsets match {
      case None =>
        log.info("Offset not initialized till now.")
        // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
      case Some(fromOffsets) =>
        log.info("Offset read from zookeeper store.")
        // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder,
          StringDecoder, (String, String)](streamingContext, kafkaParams, fromOffsets, messageHandler)
    }

    // save the offsets
    kafkaStream.foreachRDD(rdd => offsetsStoreService.saveOffsets(rdd))

    return kafkaStream
  }

  override def consume(dStream: DStream[(String, String)], windowDuration: Integer, slideDuration: Integer): DStream[(String, String)] = {
    log.info(s"FireAlertCalculatorServiceImpl.consume | windowDuration: $windowDuration; slideDuration: $slideDuration")

    if (windowDuration > 360) {
      throw new RuntimeException(("Do not pass in minutes greater than 360, protect the memory."))
    }

    return dStream.window(Seconds(60 * windowDuration), Seconds(60 * slideDuration))
  }

  override def process(windowDuration: Integer, slideDuration: Integer): Unit = super.process()

  override protected var dataFunnel: DataFunnel = new TimeRangeFunnelImpl(configManagementRepository)
  override protected var configManagementRepositoryTop: ConfigManagementRepository = configManagementRepository
  override protected var configReloadDemand: Boolean = true
}
