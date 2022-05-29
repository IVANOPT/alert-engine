package com.ivanopt.utils.alert

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.enums.AlertTypeEnum
import com.ivanopt.constant.enums.AlertTypeEnum._
import com.ivanopt.constant.{AlertQuotaConstant, KafkaSinkConstant}
import org.apache.commons.lang3.StringUtils

/**
  * Created by Ivan on 29/06/2018.
  */
object AlertSinkUtils {

  def convertCollectorSource(sourceObject: JSONObject): String = {
    val alertType: AlertTypeEnum = AlertTypeEnum.withName(sourceObject.getString(AlertQuotaConstant.ALERT_TYPE))
    val resultObject: JSONObject = new JSONObject()
    val currentTime: String = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime())
    alertType match {
      case EventCountAlert =>
      case ColumnStatsAlert =>
        resultObject.put(KafkaSinkConstant.STATS_FIELD, sourceObject.getString(AlertQuotaConstant.STATS_FIELD))
        resultObject.put(KafkaSinkConstant.STATS_TYPE, sourceObject.getString(AlertQuotaConstant.STATS_TYPE))
      case ContinuousStatsAlert =>
        resultObject.put(KafkaSinkConstant.STATS_FIELD, sourceObject.getString(AlertQuotaConstant.STATS_FIELD))
        resultObject.put(KafkaSinkConstant.FIELD_VALUE, sourceObject.getString(AlertQuotaConstant.FIELD_VALUE))
      case GroupStatsAlert =>
        resultObject.put(KafkaSinkConstant.STATS_FIELD, sourceObject.getString(AlertQuotaConstant.STATS_FIELD))
        resultObject.put(KafkaSinkConstant.GROUP_FIELD, sourceObject.getString(AlertQuotaConstant.GROUP_FIELD))
      case BaselineStatsAlert =>
        resultObject.put(KafkaSinkConstant.STATS_FIELD, sourceObject.getString(AlertQuotaConstant.STATS_FIELD))
        resultObject.put(KafkaSinkConstant.BASELINE_VALUE, sourceObject.getString(AlertQuotaConstant.BASELINE_VALUE))
        resultObject.put(KafkaSinkConstant.BASELINE_TIME_RANGE, sourceObject.getString(AlertQuotaConstant.BASELINE_TIME_RANGE))
        resultObject.put(KafkaSinkConstant.BASELINE_MAX, sourceObject.getString(AlertQuotaConstant.BASELINE_MAX))
        resultObject.put(KafkaSinkConstant.BASELINE_MIN, sourceObject.getString(AlertQuotaConstant.BASELINE_MIN))
        resultObject.put(KafkaSinkConstant.BASELINE_SECTION, sourceObject.getString(AlertQuotaConstant.BASELINE_SECTION))

      case _ =>
        throw new RuntimeException("Alert type not exist")
    }
    resultObject.put(KafkaSinkConstant.NAME, sourceObject.getString(AlertQuotaConstant.NAME))
    resultObject.put(KafkaSinkConstant.ALERT_TIME, currentTime)
    resultObject.put(KafkaSinkConstant.VALIDATION_TIME, sourceObject.getString(AlertQuotaConstant.VALIDATION_TIME))
    resultObject.put(KafkaSinkConstant.TIME_LIMIT, sourceObject.getInteger(AlertQuotaConstant.TIME_NUM)
      + sourceObject.getString(AlertQuotaConstant.TIME_UNIT))
    resultObject.put(KafkaSinkConstant.FINAL_VALUE, sourceObject.getString(AlertQuotaConstant.FINAL_VALUE))

    if (!alertType.equals(BaselineStatsAlert)) {
      resultObject.put(KafkaSinkConstant.THRESHOLD,
        AlertQuotaConstant.COMPARE_TYPE_MAP.get(sourceObject.getString(AlertQuotaConstant.COMPARE_TYPE))
          + sourceObject.getInteger(AlertQuotaConstant.THRESHOLD).toString)
    }
    resultObject.toString
  }

  def statsTypeConversionForStorage(configJson: JSONObject): JSONObject = {
    if (StringUtils.equals(IntermediateDataOpUtils.retrieveStatsType(configJson), AlertQuotaConstant.AVG)) {
      configJson.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.AVG_STORE)
    }
    if (StringUtils.equals(IntermediateDataOpUtils.retrieveStatsType(configJson), AlertQuotaConstant.BASELINE)) {
      configJson.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.BASELINE_STORE)
    }
    if (StringUtils.equals(IntermediateDataOpUtils.retrieveStatsType(configJson), AlertQuotaConstant.DISTINCT)) {
      configJson.put(AlertQuotaConstant.STATS_TYPE, AlertQuotaConstant.DISTINCT_STORE)
    }
    return configJson
  }

}
