package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils
import org.apache.spark.rdd.RDD

/**
  * Created by cong.yu on 18-6-20.
  */
class FieldValueStatsIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {

  alertService.register(AlertQuotaConstant.VALUE_COUNT, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val fieldValueDs: RDD[(String, String)] = source.loadData(rule, timeRange)
    if (fieldValueDs.isEmpty()) {
      return null
    }
    val finalValue = fieldValueDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)
    assemblyValue(rule, finalValue)
  }
}
