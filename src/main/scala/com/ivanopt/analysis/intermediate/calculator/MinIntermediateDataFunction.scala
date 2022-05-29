package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils

/**
  * Created by cong.yu on 18-6-20.
  */
class MinIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {
  alertService.register(AlertQuotaConstant.MIN, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val minDs = source.loadData(rule, timeRange)
    if (checkRDDEmpty(minDs)) {
      return null
    }
    val finalValue = minDs
      .map(pair => pair._2.toDouble)
      .reduce((v1, v2) => if (v1 < v2) {
        v1
      } else {
        v2
      })
    assemblyValue(rule, finalValue)
  }
}
