package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class AvgFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  alertService.register(AVG, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {

    val field = rule.get(STATS_FIELD).toString
    val tmpDS = ds.map(line => handle(line, field)).filter(_ != null)
    val countDS = tmpDS.count().map(line => (field, line))
    val sumDS = tmpDS.reduceByKey((v1, v2) => v1 + v2)
    val resultDS: DStream[JSONObject] = countDS.join(sumDS)
      .map(line => (line._1, line._2._2 / line._2._1))
      .map(line => assemblyValue(rule, line._2.toString))

    return resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = {
    if (null == record.get(field)) {
      log.error(s"AvgFunction.handle() | record:$record | Field:$field is not exists")
      return null
    }
    (field, record.get(field).toString.toDouble)
  }

}
