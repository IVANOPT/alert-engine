package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.analysis.Function
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 18-6-21.
  */
trait RealTimeDataCalculator extends Function[DStream[JSONObject], DStream[JSONObject]] {

  def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject]

  //TODO handle case of field values is empty
  def handle(record: JSONObject, field: String): (String, Double)

}
