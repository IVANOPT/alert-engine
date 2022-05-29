package com.ivanopt.service.impl

import com.alibaba.fastjson.JSONObject
import com.ivanopt.analysis.Function
import com.ivanopt.analysis.intermediate.calculator._
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.{AlertPublishService, IntermediateDataLoadService}

/**
  * Created by cong.yu on 18-6-20.
  */
class AlertIntermediateDataLoadViaHBaseServiceImpl()
  extends AlertPublishService[JSONObject, IntermediateDataLoadService] {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[AlertIntermediateDataLoadViaHBaseServiceImpl])

  private var functionsMap: Map[String, Function[JSONObject, IntermediateDataLoadService]] = Map()

  override def init(): AlertPublishService[JSONObject, IntermediateDataLoadService] = {
    new AvgIntermediateDataFunction(this)
    new CountIntermediateDataFunction(this)
    new SumIntermediateDataFunction(this)
    new DistinctIntermediateDataFunction(this)
    new FieldValueStatsIntermediateDataFunction(this)
    new MaxIntermediateDataFunction(this)
    new MinIntermediateDataFunction(this)
    new BaselineIntermediateDataFunction(this)
    this
  }

  override def register[T <: Function[JSONObject, IntermediateDataLoadService]](target: String, function: T): Unit = {
    functionsMap += (target -> function)
  }

  override def remove(target: String): Unit = {
    functionsMap -= target
  }

  override def notifyOb(rule: JSONObject, intermediateDataLoadService: IntermediateDataLoadService): JSONObject = {
    val target = rule.get(AlertQuotaConstant.STATS_TYPE).toString
    functionsMap.apply(target).calculate(rule, intermediateDataLoadService)
  }

}
