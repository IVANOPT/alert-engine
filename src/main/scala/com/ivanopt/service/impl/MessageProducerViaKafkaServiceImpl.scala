package com.ivanopt.service.impl

import java.util.Properties

import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.MessageProducerService
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Ivan on 04/06/2018.
  */
class MessageProducerViaKafkaServiceImpl(configManagementRepository: ConfigManagementRepository) extends MessageProducerService {

  val props: Properties = new Properties()
  props.put("bootstrap.servers", configManagementRepository.BROKER_LIST)
  props.put("key.serializer", configManagementRepository.KAFKA_KEY_SERIALIZER)
  props.put("value.serializer", configManagementRepository.KAFKA_VALUE_SERIALIZER)
  lazy val producer = new KafkaProducer[String, String](props)

  def close(kafkaProducer: KafkaProducer[String, String]): Unit = kafkaProducer.close()

  def send(topic: String, key: String, value: String): Unit = {
    val record = new ProducerRecord(topic, key, value)
    producer.send(record)
  }

  def close(): Unit = {
    producer.close()
  }

  override def publish(dStream: DStream[String], topic: String): Unit = {
    //TODO: wrap json object with value.
    dStream.foreachRDD(rdd =>
      rdd.foreach(
        record =>
          send(topic, "key", record)
      )
    )
  }

  override def publish(rdd: RDD[String], topic: String): Unit = {
    rdd.foreach(
      record => send(topic, "key", record)
    )
  }

  override def process(windowDuration: Integer, slideDuration: Integer): Unit = {
    throw new RuntimeException("Do not call process func as middle procedure.")
  }

  override protected var configReloadDemand: Boolean = true

}
