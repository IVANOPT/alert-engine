package com.ivanopt.analysis.realtime.calculator

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.service.AlertPublishService
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Ivan on 07/18/2018.
  */
class GroupCountFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  alertService.register(GROUP_COUNT, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {
    val fields: String = rule.getString(STATS_FIELD)
    val tmpDS = ds.map(line => handle(line, fields))
    val groupCountDS = tmpDS.reduceByKey((v1, v2) => v1 + v2)
    groupCountDS.count().print()
    groupCountDS.print(100)
    return groupCountDS.map(line => valueSupplied(rule, line._1, line._2))
  }

  override def handle(record: JSONObject, fields: String): (String, Double) = {
    var values: String = ""
    val fieldArr: Array[String] = fields.split(",")
    for (field <- fieldArr) {
      values = values.concat(if (record.get(field) == null) "null" else record.get(field).toString).concat(",")
    }
    return (values.concat("_").concat(fields), 1)
  }

  private def valueSupplied(rule: JSONObject, groupField: String, groupCount: Double): JSONObject = {
    val valueAssembled = assemblyValue(rule, groupCount)
    if (groupField.contains("_")) {
      val groupFieldValue = groupField.split("_")(0)
      if (groupFieldValue.endsWith(",")) {
        // replace last comma
        valueAssembled.put("groupField", groupFieldValue.replaceAll(",$", ""))
      }
    }
    return valueAssembled
  }

}