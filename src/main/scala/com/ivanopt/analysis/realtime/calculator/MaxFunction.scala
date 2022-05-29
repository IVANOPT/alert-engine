package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class MaxFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  alertService.register(MAX, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    //log.info("MaxFunction.calculate() | rule:".concat(rule.toString))

    val field = rule.get(STATS_FIELD).toString
    val resultDS: DStream[JSONObject] = ds
      .map(line => handle(line, field))
      .filter(_ != null)
      .reduceByKey((v1, v2) => if (v2 > v1) {
        v2
      } else {
        v1
      }).map(line => assemblyValue(rule, line._2.toString))

    resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = {
    if (null == record.get(field)) {
      log.error(s"MaxFunction.handle() | record:$record | Field:$field is not exists")
      return null
    }
    (field, record.get(field).toString.toDouble)
  }

}
