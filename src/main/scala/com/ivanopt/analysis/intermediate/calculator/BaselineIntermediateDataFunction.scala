package com.ivanopt.analysis.intermediate.calculator

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils
import org.apache.spark.rdd.RDD

/**
  * Created by cong.yu on 18-6-25.
  */
class BaselineIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {

  alertService.register(AlertQuotaConstant.BASELINE, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val comTimeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val baselineTimeRange = IntermediateDataOpUtils.parseBaselineRuleToTimeRange(rule)
    rule.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.SUM)
    val comSumDs: RDD[(String, String)] = source.loadData(rule, comTimeRange)
    if (checkRDDEmpty(comSumDs)) {
      return null
    }
    val comSumFinalValue = comSumDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)

    val baselineSumDs: RDD[(String, String)] = source.loadData(rule, baselineTimeRange)
    if (checkRDDEmpty(baselineSumDs)) {
      return null
    }
    val baselineSumValue = baselineSumDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)

    rule.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.COUNT)
    val comCountDs: RDD[(String, String)] = source.loadData(rule, comTimeRange)
    if (checkRDDEmpty(comCountDs)) {
      return null
    }
    val comCountFinalValue = comCountDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)

    val baselineCountDs: RDD[(String, String)] = source.loadData(rule, baselineTimeRange)
    if (checkRDDEmpty(baselineCountDs)) {
      return null
    }
    val baselineCountValue = baselineCountDs
      .map(pair => pair._2.toDouble)
      .reduce(_ + _)
    rule.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.BASELINE)

    assemblyValue(rule, (comSumFinalValue / comCountFinalValue, baselineSumValue / baselineCountValue))
  }

  override def assemblyValue(rule: JSONObject, value: Any): JSONObject = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    rule.put("alertTime", sdf.format(new Date()))
    rule.put("finalValue", value.asInstanceOf[(Double, Double)]._1)
    rule.put("baselineValue", value.asInstanceOf[(Double, Double)]._2)
    rule
  }

}
