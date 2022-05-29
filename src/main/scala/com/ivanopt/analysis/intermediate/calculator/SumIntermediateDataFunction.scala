package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils

/**
  * Created by cong.yu on 18-6-20.
  */
class SumIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {

  alertService.register(AlertQuotaConstant.SUM, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val sumDs = source.loadData(rule, timeRange)
    if (checkRDDEmpty(sumDs)) {
      return null
    }
    val finalValue = sumDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)
    assemblyValue(rule, finalValue)
  }

}
