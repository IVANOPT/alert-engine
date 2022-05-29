package com.ivanopt.constant.enums

/**
  * Created with IDEA
  * User: zzzz76
  * Date: 2018-06-21
  */
object AlertTypeEnum extends Enumeration {
  type AlertTypeEnum = Value
  val EventCountAlert, ColumnStatsAlert, ContinuousStatsAlert, GroupStatsAlert, BaselineStatsAlert = Value
}
