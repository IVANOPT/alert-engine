package com.ivanopt.service

import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 11/05/2018.
  */
trait OffsetsStoreService extends Serializable {

  def readOffsets(): Option[Map[TopicAndPartition, Long]]

  def saveOffsets(rdd: RDD[_]): Unit

}
