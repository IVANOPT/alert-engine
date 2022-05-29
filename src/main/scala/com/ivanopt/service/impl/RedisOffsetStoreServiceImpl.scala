package com.ivanopt.service.impl

import com.ivanopt.service.OffsetsStoreService
import kafka.common.TopicAndPartition
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import redis.clients.jedis.Jedis

/**
  * Created by Ivan on 20/05/2018.
  */
class RedisOffsetStoreServiceImpl(topic: String, redisAddr: String) extends OffsetsStoreService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[RedisOffsetStoreServiceImpl]);
  val jedis: Jedis = new Jedis(redisAddr)

  override def readOffsets(): Option[Map[TopicAndPartition, Long]] = {

    log.info("Reading offsets from Redis")
    var offsetsRanges: String = jedis.get(topic)
    if (StringUtils.isBlank(offsetsRanges)) {
      return None
    }

    val offsets: Map[TopicAndPartition, Long] = offsetsRanges.split(",")
      .map(s => s.split(":"))
      .map { case Array(partitionStr, offsetsStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetsStr.toLong) }
      .toMap

    return Some(offsets)
  }

  override def saveOffsets(rdd: RDD[_]): Unit = {

    log.info("Saving offsets to Redis")

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // Using OffsetRange(topic: 'streaming-data-tool', partition: 0, range: [3886560 -> 9298223])
    offsetsRanges.foreach(offsetsRange => log.info(s"Using ${offsetsRange}"))

    val offsetsRangesStr: String = offsetsRanges.map(offsetsRange => s"${offsetsRange.partition}:${offsetsRange.fromOffset}")
      .mkString(",")

    log.info(s"Writing offsets to Redis with value: ${offsetsRangesStr}")

    val offsetsKey: String = topic
    log.info(s"Writing offsets to Redis with key: ${offsetsKey}")

    jedis.set(offsetsKey, offsetsRangesStr)

  }

}
