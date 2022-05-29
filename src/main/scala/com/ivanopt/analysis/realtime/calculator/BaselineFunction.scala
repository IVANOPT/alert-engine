package com.ivanopt.analysis.realtime.calculator

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant._
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.AlertPublishService
import com.ivanopt.service.impl.IntermediateDataLoadViaHBaseServiceImpl
import com.ivanopt.utils.alert.IntermediateDataOpUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by cong.yu on 08/06/2018.
  */
class BaselineFunction[T <: AlertPublishService[DStream[JSONObject], DStream[JSONObject]]](alertService: T)
  extends RealTimeDataCalculator {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[BaselineFunction[T]])

  alertService.register(BASELINE, this)

  override def calculate(rule: JSONObject, ds: DStream[JSONObject]): DStream[JSONObject] = {

    var resultDS: DStream[JSONObject] = null

    val field = rule.get(STATS_FIELD).toString
    val tmpDS = ds.map(line => handle(line, field)).filter(_ != null)
    val countDS = tmpDS.count().map(line => (field, line))
    val sumDS = tmpDS.reduceByKey((v1, v2) => v1 + v2)
    val avgDS = countDS.join(sumDS)
      .map(line => (line._1, line._2._2 / line._2._1))
    val configManagementRepository = new ConfigManagementRepository()
    val sc = SparkContext.getOrCreate()
    println(s"MARK(BaselineFunction): $sc")
    val intermediateDataLoadService = new IntermediateDataLoadViaHBaseServiceImpl(configManagementRepository, sc)

    val baselineTimeRange = IntermediateDataOpUtils.parseBaselineRuleToTimeRange(rule)
    rule.put(STATS_TYPE, COUNT)
    val hbaseCountRDD: RDD[(String, String)] = intermediateDataLoadService.loadData(rule, baselineTimeRange)

    if(hbaseCountRDD.isEmpty()) {
      return resultDS
    }

    val hbaseCount = hbaseCountRDD.map(tuple => tuple._2.toDouble).reduce(_ + _)
    rule.put(STATS_TYPE, SUM)
    val hbaseSumRDD = intermediateDataLoadService.loadData(rule, baselineTimeRange)

    if (hbaseSumRDD.isEmpty()) {
      return resultDS
    }

    val hbaseSum = hbaseSumRDD.map(tuple => tuple._2.toDouble).reduce(_ + _)
    val hbaseAvg = hbaseSum / hbaseCount
    rule.put(STATS_TYPE, BASELINE)
    resultDS = avgDS.map(line => assemblyValue(rule, (line._2, hbaseAvg)))

    return resultDS
  }

  override def handle(record: JSONObject, field: String): (String, Double) = {
    if (null == record.get(field)) {
      log.error(s"BaselineFunction.handle() | record:$record | Field:$field is not exists")
      return null
    }
    (field, record.get(field).toString.toDouble)
  }

  override def assemblyValue(rule: JSONObject, value: Any): JSONObject = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    rule.put("alertTime", sdf.format(new Date()))
    rule.put("finalValue", value.asInstanceOf[(Double, Double)]._1)
    rule.put("baselineValue", value.asInstanceOf[(Double, Double)]._2)
    rule
  }
}
