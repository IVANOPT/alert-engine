package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils
import org.apache.spark.rdd.RDD

/**
  * Created by cong.yu on 18-6-20.
  */
class AvgIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {

  alertService.register(AlertQuotaConstant.AVG, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    rule.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.SUM)
    val sumDs: RDD[(String, String)] = source.loadData(rule, timeRange)
    if (checkRDDEmpty(sumDs)) {
      return null
    }
    val sumFinalValue = sumDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)

    rule.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.COUNT)
    val countDs: RDD[(String, String)] = source.loadData(rule, timeRange)
    if (checkRDDEmpty(countDs)) {
      return null
    }
    val countFinalValue = countDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)

    assemblyValue(rule, sumFinalValue / countFinalValue)

  }

}
