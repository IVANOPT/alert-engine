package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils
import org.apache.spark.rdd.RDD


/**
  * Created by cong.yu on 18-6-20.
  */
class CountIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {

  alertService.register(AlertQuotaConstant.COUNT, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val countDs: RDD[(String, String)] = source.loadData(rule, timeRange)
    if (checkRDDEmpty(countDs)) {
      return null
    }

    val finalValue = countDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)
    assemblyValue(rule, finalValue)
  }
}
