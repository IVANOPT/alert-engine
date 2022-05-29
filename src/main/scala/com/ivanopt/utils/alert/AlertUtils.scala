package com.ivanopt.utils.alert

import com.alibaba.fastjson.JSONObject
import com.googlecode.aviator.AviatorEvaluator
import com.ivanopt.constant.AlertQuotaConstant._

/**
  * Created by cong.yu on 19/06/2018.
  */
object AlertUtils {
  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(AlertUtils.getClass)

  def execute(result: JSONObject, alertRule: JSONObject): Boolean = {

    val expression = if (alertRule.get(STATS_TYPE).equals(BASELINE)) {
      parseBaselineRuleToExpression(result, alertRule)
    } else {
      val compareType = COMPARE_TYPE_MAP(alertRule.get(COMPARE_TYPE).toString)
      val value = result.get(FINAL_VALUE).toString
      val threshold = alertRule.get(THRESHOLD).toString
      s"$value$compareType$threshold"
    }
    log.info("AlertUtils.execute() | Aviator expression： ".concat(expression))
    AviatorEvaluator.execute(expression).asInstanceOf[Boolean]
  }

  def parseBaselineRuleToExpression(result: JSONObject, alertRule: JSONObject): String = {
    val value = result.get(FINAL_VALUE).toString
    val baselineValue = result.get("baselineValue").toString.toDouble
    //TODO 100% or 100
    val baselineMinValue = alertRule.get("baselineMin").toString.toDouble * baselineValue
    val baselineMaxValue = alertRule.get("baselineMax").toString.toDouble * baselineValue
    val baselineSection = alertRule.get("baselineSection").toString
    val expression = if ("left".equals(baselineSection)) {
      s"$value<$baselineMinValue"
    } else if ("middle".equals(baselineSection)) {
      s"$value>=$baselineMinValue&&$value<=$baselineMaxValue"
    } else if ("right".equals(baselineSection)) {
      s"$value>$baselineMaxValue"
    }
    expression.toString
  }

  def assemblyError(rule: JSONObject, message: String): JSONObject = {
    val errorJSON = new JSONObject()
    errorJSON.put("error", message)
    errorJSON.put("name", rule.get("name"))
    errorJSON
  }
}
