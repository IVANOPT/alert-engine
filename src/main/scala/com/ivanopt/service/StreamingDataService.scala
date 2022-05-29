package com.ivanopt.service

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ivan on 07/05/2018.
  */
@SerialVersionUID(101L)
class JSONParser extends Serializable {

  def parse(str: String): JSONObject = {
    return JSON.parseObject(str)
  }

}

trait StreamingDataService extends Serializable {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[StreamingDataService])
  protected var configReloadDemand: Boolean

  def stopStreamingContext(streamingContext: StreamingContext): Unit = {
    import sys.process._
    Seq("/bin/sh", "-c", "ps -ef | grep spark | awk '{print $2}' | xargs kill -SIGTERM").!!
    // streamingContext.stop(false, true)
  }

  def startupStreamingContext(streamingContext: StreamingContext): Unit = {
    log.info("streamingContext.start()")
    streamingContext.start()
    log.info("streamingContext.awaitTerminationOrTimeout()")
    streamingContext.awaitTermination()
  }

  def initSparkContext(): SparkContext = {
    log.info("ivanopt logger | initSparkContext()")
    val sparkConf: SparkConf = new SparkConf().setAppName("ivanopt-alert-service")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    return new SparkContext(sparkConf)
  }

  def initStreamingContext(sc: SparkContext): StreamingContext = {
    log.info("ivanopt logger | initStreamingContext()")
    var sparkContext: SparkContext = sc
    if (sc == null) {
      sparkContext = initSparkContext()
    }
    val streamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(5))
    // streamingContext.checkpoint(new ConfigManagementRepository().checkPoint)
    return streamingContext
  }

  def process(windowDuration: Integer, slideDuration: Integer)

}
