package com.ivanopt.constant

/**
  * Created by cong.yu on 15/06/2018.
  */
object AlertQuotaConstant {

  final val ALERT_TYPE: String = "alertType"
  final val NAME: String = "name"
  final val VALIDATION_TIME: String = "validationTime"
  final val ALERT_ID: String = "id"
  final val TIME_NUM: String = "timeNum"
  final val TIME_UNIT: String = "timeUnit"
  final val STATS_TYPE: String = "statsType"
  final val STATS_FIELD: String = "statsField"
  final val GROUP_FIELD: String = "groupField"
  final val COMPARE_TYPE: String = "compareType"
  final val THRESHOLD: String = "threshold"
  final val FIELD_VALUE: String = "fieldValue"
  final val DISTINCT_FIELD: String = "distinctField"
  final val BASELINE_VALUE: String = "baselineValue"
  final val BASELINE_TIME_RANGE: String = "baselineTimeRange"
  final val BASELINE_MAX: String = "baselineMax"
  final val BASELINE_MIN: String = "baselineMin"
  final val BASELINE_SECTION: String = "baselineSection"

  final val FINAL_VALUE: String = "finalValue"

  // In conversion status
  final val AVG_STORE = "avg_store"
  final val AVG = "avg"
  final val DISTINCT = "distinct"
  final val DISTINCT_STORE = "distinct_store"
  final val COUNT = "count"
  final val MAX = "max"
  final val MIN = "min"
  final val SUM = "sum"
  final val VALUE_COUNT = "value_count"
  final val BASELINE = "baseline"
  final val BASELINE_STORE = "baseline_store"
  final val GROUP_COUNT = "group_count"

  final val COMPARE_TYPE_MAP: Map[String, String] = Map(
    "eq" -> "==", "gt" -> ">", "lt" -> "<", "ge" -> ">=", "le" -> "<=", "ne" -> "!="
  )

}
