package com.ivanopt.utils.aviator

import com.alibaba.fastjson.JSONObject

trait IAviatorFunction {

  def conversion(filterRule: String): String

  def execute(afterConversionRule: String, jsonObject: JSONObject): Boolean
}
