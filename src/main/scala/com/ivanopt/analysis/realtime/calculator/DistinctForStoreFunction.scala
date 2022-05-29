package com.ivanopt.analysis.realtime.calculator

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class DistinctForStoreFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator with Serializable {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  alertService.register(DISTINCT_STORE, this)

  // assembly distinct value "a,b,c,d"
  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    val field = rule.get(STATS_FIELD).toString
    val distinctFieldDS: DStream[(String, String)] = ds.map(line => handle(line, field))
      .filter(_ != null)
      .reduceByKey((v1, v2) => v1 + v2).map(value => (field, value._1))
    val distinctCountDS = distinctFieldDS.count()
      .map(line => (field, line))
    val resultDS = distinctFieldDS.join(distinctCountDS).map(pair => assemblyValue(rule, pair))
    resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = {
    if (null == record.get(field)) {
      log.error(s"DistinctForStoreFunction.handle() | record:$record | Field:$field is not exists")
      return null
    }
    (record.get(field).toString, 1L)
  }

  override def assemblyValue(rule: JSONObject, value: Any): JSONObject = {
    println("DistinctFunction.assemblyValue() | value: ".concat(value.toString))
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ruleParser = rule.clone().asInstanceOf[JSONObject]
    ruleParser.put("alertTime", sdf.format(new Date()))
    ruleParser.put("finalValue", value.asInstanceOf[(String, (String, Long))]._2._2)
    ruleParser.put("distinctField", value.asInstanceOf[(String, (String, Long))]._2._1)
    println("DistinctFunction.assemblyValue() | ruleParser: ".concat(ruleParser.toString))
    ruleParser
  }

}
