package com.ivanopt.utils.alert

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.{AlertQuotaConstant, TimeConstant}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Base64

/**
  * Created by cong.yu on 19/06/2018.
  */
object IntermediateDataOpUtils {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(IntermediateDataOpUtils.getClass)

  val sdf = new SimpleDateFormat("yyyyMMddHHmm")

  def convertScanToString(scan: Scan): String = {
    val out = new ByteArrayOutputStream
    val dos = new DataOutputStream(out)
    scan.write(dos)
    Base64.encodeBytes(out.toByteArray)
  }


  def parseRuleToTimeRange(rule: JSONObject): (String, String) = {

    val timeNum = rule.get(AlertQuotaConstant.TIME_NUM).asInstanceOf[java.lang.Integer]
    val timeUnit = rule.get(AlertQuotaConstant.TIME_UNIT).asInstanceOf[String]
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MINUTE, 1)
    val stopTime = sdf.format(calendar.getTime)
    calendar.add(Calendar.MINUTE, -1)
    timeUnit match {
      case TimeConstant.MIN => calendar.add(Calendar.MINUTE, -timeNum)
      case TimeConstant.HOUR => calendar.add(Calendar.HOUR_OF_DAY, -timeNum)
      case TimeConstant.DAY => calendar.add(Calendar.DAY_OF_MONTH, -timeNum)
      case _ => throw new IllegalArgumentException(s"can't support time unit : $timeUnit")
    }
    val startTime = sdf.format(calendar.getTime)
    log.info(s"startTime: $startTime, stopTime: $stopTime")

    (startTime, stopTime)
  }

  def parseBaselineRuleToTimeRange(rule: JSONObject): (String, String) = {
    val baselineTimeRange = rule.get("baselineTimeRange")
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    baselineTimeRange match {
      case "yesterday" =>
        generateYesterdayTimeRange(calendar)
      case "lastweek" =>
        generateLastWeekTimeRange(calendar)
      case "lastmonth" =>
        generateLastMonthTimeRange(calendar)
      case _ => throw new IllegalArgumentException("Undetermined time range")
    }
  }

  def generateYesterdayTimeRange(calendar: Calendar): (String, String) = {
    val stopTime = sdf.format(calendar.getTime)
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    val startTime = sdf.format(calendar.getTime)
    (startTime, stopTime)
  }

  def generateLastWeekTimeRange(calendar: Calendar): (String, String) = {
    var fixDayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1
    calendar.add(Calendar.DATE, -fixDayOfWeek - 7)
    if (0 == fixDayOfWeek) fixDayOfWeek = 7
    calendar.add(Calendar.DATE, 1)
    val startTime = sdf.format(calendar.getTime)
    calendar.add(Calendar.DATE, 6)
    val stopTime = sdf.format(calendar.getTime)
    (startTime, stopTime)
  }

  def generateLastMonthTimeRange(calendar: Calendar): (String, String) = {
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    val stopTime = sdf.format(calendar.getTime)
    calendar.add(Calendar.MONTH, -1)
    val startTime = sdf.format(calendar.getTime)
    (startTime, stopTime)
  }

  def retrieveTimeRange(config: JSONObject): java.lang.Integer = {
    return config.get(AlertQuotaConstant.TIME_NUM).asInstanceOf[java.lang.Integer]
  }

  def retrieveTimeUnit(config: JSONObject): String = {
    return config.get(AlertQuotaConstant.TIME_UNIT).asInstanceOf[String]
  }

  def retrieveStatsType(config: JSONObject): String = {
    return config.get(AlertQuotaConstant.STATS_TYPE).asInstanceOf[String]
  }

  def retrieveFinalValue(config: JSONObject): String = {
    return String.valueOf(config.get(AlertQuotaConstant.FINAL_VALUE))
  }

}
