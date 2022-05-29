package com.ivanopt.utils.alert

import java.util.Calendar

import com.alibaba.fastjson.JSON
import com.ivanopt.utils.alert.IntermediateDataOpUtils.{generateLastMonthTimeRange, generateLastWeekTimeRange, generateYesterdayTimeRange}
import org.testng.annotations.Test

/**
  * Created by cong.yu on 19/06/2018.
  */
class IntermediateDataOpUtilsTest {

  val rule = "{\"id\":\"2\",\"alertTypeId\":\"2\",\"timeNum\":\"10\",\"timeUnit\":\"min\",\"statsField\":\"columnNum\",\"statsType\":\"max\",\"compareType\":\"gt\",\"threshold\":\"20\"}"

  @Test
  def parseRuleToTimeRange(): Unit = {
    val timeRange = IntermediateDataOpUtils.parseRuleToTimeRange(JSON.parseObject(rule))
    println(timeRange)
  }

  @Test
  def parseBaselineRuleToTimeRange(): Unit = {
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    println(generateYesterdayTimeRange(calendar))
    println(generateLastWeekTimeRange(calendar))
    println(generateLastMonthTimeRange(calendar))
  }
}
