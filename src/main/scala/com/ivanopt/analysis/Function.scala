package com.ivanopt.analysis

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject

/**
  * Created by cong.yu on 08/06/2018.
  */
trait Function[T, R] extends Serializable {
  def calculate(rule: JSONObject, source: R): T

  def assemblyValue(rule: JSONObject, value: Any): JSONObject = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    rule.put("alertTime", sdf.format(new Date()))
    //TODO Use the value of a special flag to distinguish the key of original json
    rule.put("finalValue", value)
    rule
  }
}
