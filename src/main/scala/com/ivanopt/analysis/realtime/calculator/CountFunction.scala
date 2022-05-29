package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class CountFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[CountFunction[T]])

  alertService.register(COUNT, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    log.info("CountFunction.calculate() | record:$record | rule:".concat(rule.toString))
    val resultDS: DStream[JSONObject] = ds.count().map(line => assemblyValue(rule, line.toString))
    resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = ???

}
