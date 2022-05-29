package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.{AlertQuotaConstant, IntermediateDataConstant}
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}
import com.ivanopt.utils.alert.IntermediateDataOpUtils
import org.apache.spark.rdd.RDD


/**
  * Created by cong.yu on 18-6-21.
  */
class DistinctIntermediateDataFunction[T <: AlertPublishService[JSONObject, IntermediateDataLoadService]](alertService: T)
  extends IntermediateDataCalculator {

  alertService.register(AlertQuotaConstant.DISTINCT_STORE, this)

  override def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(rule)
    val distinctDs: RDD[(String, String)] = source.loadData(rule, timeRange)
    if (checkRDDEmpty(distinctDs)) {
      return null
    }
    val finalValue = distinctDs
      .map(tuple => tuple._1.split(IntermediateDataConstant.ROWKEY_DELIMIT)(2))
      .distinct()
      .count()
    assemblyValue(rule, finalValue)

  }
}
