package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 18-6-26.
  */
class AvgForStoreFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  alertService.register(AVG_STORE, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    val field: String = rule.get(STATS_FIELD).asInstanceOf[String]
    val tmpDS = ds.map(line => handle(line, field)).filter(_ != null)
    val ruleCountParser = rule.clone().asInstanceOf[JSONObject]
    ruleCountParser.put(STATS_TYPE, COUNT)
    val countDS = tmpDS.count().map(line => (field, line)).map(tuple => assemblyValue(ruleCountParser, tuple._2))

    val ruleSumParser = rule.clone().asInstanceOf[JSONObject]
    ruleSumParser.put(STATS_TYPE, SUM)
    val sumDS = tmpDS.reduceByKey((v1, v2) => v1 + v2).map(tuple => assemblyValue(ruleSumParser, tuple._2))
    val resultDS = countDS.union(sumDS)
    resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = {
    if (null == record.get(field)) {
      log.error(s"AvgForStoreFunction.handle() | record:$record | Field:$field is not exists")
      return null
    }
    (field, record.get(field).toString.toDouble)
  }

}

