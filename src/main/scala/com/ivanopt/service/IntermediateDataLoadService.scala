package com.ivanopt.service


import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD

trait IntermediateDataLoadService extends Serializable {

  def loadData(rule: JSONObject, timeRange: (String, String)): RDD[(String, String)]

}
