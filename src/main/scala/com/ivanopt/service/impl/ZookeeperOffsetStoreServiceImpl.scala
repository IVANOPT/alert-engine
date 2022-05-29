package com.ivanopt.service.impl

import com.ivanopt.service.OffsetsStoreService
import com.ivanopt.utils.zookeeper.ZkConnect
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.zookeeper.ZooKeeper

/**
  * Created by Ivan on 11/05/2018.
  */
@SerialVersionUID(1102L)
class ZookeeperOffsetStoreServiceImpl(topic: String, zkAddr: String, childPath: String) extends OffsetsStoreService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[ZookeeperOffsetStoreServiceImpl])
  private final val offsetsKey: String = "/" + topic + "_offsets" + childPath
  val connector: ZkConnect = new ZkConnect()
  val zk: ZooKeeper = connector.connect(zkAddr)

  // Read the previously saved offsets from Zookeeper
  override def readOffsets(): Option[Map[TopicAndPartition, Long]] = {

    log.info("Reading offsets from ZooKeeper")
    if (zk.exists(offsetsKey, false) == null) {
      return None
    }

    val offsetsRangesInBytes: Array[Byte] = zk.getData(offsetsKey, true, zk.exists(offsetsKey, true))
    val offsetsRanges: String = (offsetsRangesInBytes.map(_.toChar)).mkString

    val offsets: Map[TopicAndPartition, Long] = offsetsRanges.split(",")
      .map(s => s.split(":"))
      .map { case Array(partitionStr, offsetsStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetsStr.toLong) }
      .toMap

    return Some(offsets)

  }

  // Save the offsets back to ZooKeeper
  //
  // IMPORTANT: We're not saving the offset immediately but instead save the offset from the previous batch. This is
  // because the extraction of the offsets has to be done at the beginning of the stream processing, before the real
  // logic is applied. Instead, we want to save the offsets once we have successfully processed a batch, hence the
  // workaround.
  override def saveOffsets(rdd: RDD[_]): Unit = {

    log.info("Saving offsets to Zookeeper")

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // Using OffsetRange(topic: 'streaming-data-tool', partition: 0, range: [3886560 -> 9298223])
    offsetsRanges.foreach(offsetsRange => log.info(s"Using ${offsetsRange}"))

    val offsetsRangesStr = offsetsRanges.map(offsetsRange => s"${offsetsRange.partition}:${offsetsRange.fromOffset}")
      .mkString(",")
    log.info(s"Writing offsets to Zookeeper: ${offsetsRangesStr}")

    if (zk.exists(offsetsKey, false) == null) {
      connector.createNode(offsetsKey, offsetsRangesStr.getBytes())
    } else {
      connector.updateNode(offsetsKey, offsetsRangesStr.getBytes())
    }

  }

}

