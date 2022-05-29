package com.ivanopt.service.impl

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.{IntermediateDataStoreService, MessageConsumerService, OffsetsStoreService}
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by Ivan on 07/05/2018.
  */
@SerialVersionUID(100L)
class MessageConsumerViaKafkaServiceImpl(configManagementRepository: ConfigManagementRepository) extends MessageConsumerService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[MessageConsumerViaKafkaServiceImpl])

  override def subscribe(streamingContext: StreamingContext): DStream[(String, String)] = {

    val topicList = configManagementRepository.SOURCE_TOPIC
    val brokerList = configManagementRepository.BROKER_LIST
    val groupId = configManagementRepository.GROUP_ID
    val zkList = configManagementRepository.ZK_LIST
    val kafkaParams: Map[String, String] = Map("bootstrap.servers" -> brokerList, "group.id" -> groupId)
    val topics = Set(topicList)

    val offsetsStoreService: OffsetsStoreService = new ZookeeperOffsetStoreServiceImpl(topicList, zkList, "consumer")
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

  override def subscribeInMinutes(streamingContext: StreamingContext, windowDuration: Integer, slideDuration: Integer): DStream[(String, String)] = {
    if (windowDuration > 60) {
      throw new RuntimeException("Do not pass in minutes greater than 60, protect the memory.")
    }
    return subscribe(streamingContext).window(Seconds(60 * windowDuration), Seconds(60 * slideDuration))
  }

  override def intermediateDataStoreServiceSelector(): IntermediateDataStoreService = {
    return new IntermediateDataStoreViaHBaseServiceImpl(configManagementRepository)
  }

  override protected var configReloadDemand: Boolean = true
}
