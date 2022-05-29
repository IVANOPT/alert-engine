package com.ivanopt.service.impl


import com.alibaba.fastjson.JSONObject
import com.ivanopt.analysis.Function
import com.ivanopt.analysis.realtime.calculator._
import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class AlertRealTimeDataServiceImpl extends AlertPublishService[DStream[JSONObject], DStream[JSONObject]] {

  private var functionsMap: Map[String, Function[DStream[JSONObject], DStream[JSONObject]]] = Map()

  override def register[T <: Function[DStream[JSONObject], DStream[JSONObject]]](target: String, function: T): Unit = {
    functionsMap += (target -> function)
  }

  override def remove(target: String): Unit = {
    functionsMap -= target
  }

  override def notifyOb(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    val target = rule.get(AlertQuotaConstant.STATS_TYPE).toString
    functionsMap(target).calculate(rule, ds)
  }

  override def init(): AlertRealTimeDataServiceImpl = {
    new CountFunction(this)
    new AvgFunction(this)
    new AvgForStoreFunction(this)
    new SumFunction(this)
    new MaxFunction(this)
    new MinFunction(this)
    new DistinctForStoreFunction(this)
    new DistinctFunction(this)
    new FieldValueStatsFunction(this)
    new BaselineFunction(this)
    new BaselineForStoreFunction(this)
    new GroupCountFunction(this)
    return this
  }

}
