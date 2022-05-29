package com.ivanopt.service

import com.alibaba.fastjson.JSONObject
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by Ivan on 20/06/2018.
 */
trait DataFunnel extends Serializable {

  protected def condition(): String

  protected def target(item: JSONObject): java.lang.Long

  protected def generationMatched(target: java.lang.Long, threshold: java.lang.Integer, unit: String): Boolean

  def filterByCondition(ds: DStream[JSONObject], threshold: java.lang.Integer, unit: String): DStream[JSONObject] = {
    return ds.filter(item => generationMatched(target(item), threshold, unit))
  }

}