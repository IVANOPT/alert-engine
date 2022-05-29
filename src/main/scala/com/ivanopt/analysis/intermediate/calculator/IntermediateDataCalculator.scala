package com.ivanopt.analysis.intermediate.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.analysis.Function
import com.ivanopt.service.IntermediateDataLoadService
import org.apache.spark.rdd.RDD

/**
  * Created by cong.yu on 18-6-20.
  */
trait IntermediateDataCalculator extends Function[JSONObject, IntermediateDataLoadService] {

  def calculate(rule: JSONObject, source: IntermediateDataLoadService): JSONObject

  protected def checkRDDEmpty(rdd: RDD[(String, String)]): Boolean = {
    return rdd.isEmpty()
  }

}
