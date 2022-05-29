package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils

/**
  * Created by cong.yu on 18-6-20.
  */
class MaxIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {
  alertService.register(AlertQuotaConstant.MAX, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val maxDs = source.loadData(rule, timeRange)
    if (checkRDDEmpty(maxDs)) {
      return null
    }
    val finalValue = maxDs
      .map(pair => pair._2.toDouble)
      .reduce((v1, v2) => if (v1 > v2) {
        v1
      } else {
        v2
      })
    assemblyValue(rule, finalValue)
  }
}
