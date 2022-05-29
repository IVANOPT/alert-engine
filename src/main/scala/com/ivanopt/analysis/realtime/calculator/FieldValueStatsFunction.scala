package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class FieldValueStatsFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  alertService.register(VALUE_COUNT, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {

    val field = rule.get(STATS_FIELD).toString
    val fieldValue = rule.get(FIELD_VALUE)
    val resultDS: DStream[JSONObject] = ds.map(line => handle(line, field))
      .filter(_ != null)
      .filter(line => fieldValue.equals(line._1))
      .count()
      .map(line => assemblyValue(rule, line.toString))

    resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = {
    if (null == record.get(field)) {
      log.error(s"FieldValueStatsFunction.handle() | record:$record | Field:$field is not exists")
      return null
    }
    (record.get(field).toString, 1)
  }

}
