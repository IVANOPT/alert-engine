package com.ivanopt.service.impl

import com.alibaba.fastjson.{ JSON, JSONObject }
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.utils.alert.AlertSinkUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.testng.annotations.Test

/**
 * Created with IDEA
 * User: zzzz76
 * Date: 2018-06-19
 */
class FireAlertCalculatorServiceImplTest {
  val configManagementRepository = new ConfigManagementRepository
  val fireAlertCalculatorServiceImpl = new FireAlertCalculatorServiceImpl(configManagementRepository)

  @Test(groups = Array("intermediate-data-store"))
  def convertCollectorSource(): Unit = {
    val source = "{\"alertType\":\"ColumnStatsAlert\",\"name\":\"客户端请求\",\"compareType\":\"gt\",\"statsField\":\"columnNum\",\"timeNum\":\"5\",\"statsType\":\"count\",\"threshold\":\"10\",\"id\":\"5\",\"timeUnit\":\"min\",\"version\":\"0.1\",\"validationTime\":\"2018\",finalValue:\"78\"}"
    val jsonObject = JSON.parseObject(source)
    println(AlertSinkUtils.convertCollectorSource(jsonObject))
  }

  @Test(groups = Array("intermediate-data-store"))
  def alertCollector(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    //Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    val alertDs: DStream[JSONObject] = lines.map(json => JSON.parseObject(json))
    fireAlertCalculatorServiceImpl.alertCollector(alertDs)
    ssc.start()
    ssc.awaitTermination()
  }
}
