package com.ivanopt.analysis

import java.io.{File, FileOutputStream}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ivanopt.service.impl.AlertRealTimeDataServiceImpl
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.testng.annotations.Test

class AlertServiceTest {

  @Test
  def process(): Unit = {
    fileProvider()
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("observer_test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("/home/t1mon/tmp/ssc/ssc_checkpoint")
    val ds: DStream[JSONObject] = ssc.textFileStream("/home/t1mon/tmp/ssc/ssc_test").map(line => JSON.parseObject(line))
    val rule = "{\"id\":\"2\",\"alertTypeId\":\"2\",\"timeNum\":\"10\",\"timeUnit\":\"min\",\"statsField\":\"columnNum\",\"statsType\":\"sum\",\"compareType\":\"gt\",\"threshold\":\"20\"}"
    val alertService = new AlertRealTimeDataServiceImpl()
    alertService.init()
    alertService.notifyOb(JSON.parseObject(rule), ds).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def fileProvider(): Unit = {
    val streamFile = new Thread(new Runnable {
      override def run(): Unit = {
        for (i <- 1 to 1000) {
          Thread.sleep(500)
          val file = new File(s"/home/t1mon/tmp/ssc/ssc_test/test$i".concat(".txt"))
          val fos = new FileOutputStream(file)
          val record = "{\"owner\":\"DSG\",\"operationType\":\"I\",\"columnNum\":\"" + i + "\"}" + "\n" + "{\"owner\":\"DSG\",\"operationType\":\"I\",\"columnNum\":\"" + i + "\"}"
          fos.write(record.getBytes())
          fos.flush()
          fos.close()
        }
      }
    })
    streamFile.start()
  }

}
