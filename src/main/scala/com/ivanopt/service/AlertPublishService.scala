package com.ivanopt.service

import com.alibaba.fastjson.JSONObject
import com.ivanopt.analysis.Function

/**
  * Created by cong.yu on 08/06/2018.
  */
trait AlertPublishService[T, R] extends Serializable{

  def init(): AlertPublishService[T, R]

  def register[U <: Function[T, R]](target: String, function: U)

  def remove(target: String)

  def notifyOb(rule: JSONObject, source: R): T

}
