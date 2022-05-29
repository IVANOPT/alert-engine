package com.ivanopt.utils.aviator

import java.security.InvalidParameterException
import java.util
import java.util.regex.Pattern

import com.alibaba.fastjson.JSONObject
import com.googlecode.aviator.{AviatorEvaluator, Expression}

import scala.collection.mutable


object AviatorFunction extends IAviatorFunction {

  private final val log: org.slf4j.Logger =
    org.slf4j.LoggerFactory.getLogger(AviatorFunction.getClass)

  override def execute(afterConversionRule: String, jsonObject: JSONObject): Boolean = {
    //    val afterConversionRule: String = "IP>10.123.1.0 &&  (MAC==' '  || MAC=='unknown') && IP>=10.123.1.0 && IP<=10.123.1.0 && IP =~ a || IP != a"
    val compileExp: Expression = AviatorEvaluator.compile(afterConversionRule)

    val filterFields: mutable.Set[String] = getFieldOnFilter(afterConversionRule)
    var env: util.Map[String, Object] = new util.HashMap[String, Object]()
    for (field <- filterFields) {
      env.put(field, jsonObject.get(field))
    }
    log.info(env.toString)
    val result: Boolean = compileExp.execute(env).asInstanceOf[Boolean]
    return result
  }

  // TODO: check keyword in original filter string, eg: like, %, <> ...
  override def conversion(filterRule: String): String = {
    var rule = filterRule.replace(">=", "largerOrEqual")
      .replace("<=", "lessOrEqual")
      .replace("=", "==")
      .replace("<>", "!=")
      .replace("largerOrEqual", ">=")
      .replace("lessOrEqual", "<=")
      .replace("AND", "&&")
      .replace("OR", "||")
      .replace("LIKE", "=~")

    // to handle %
    val slices = rule.split("&&|\\|\\|")
    var conversionMap: Map[String, String] = Map[String, String]()
    for (slice <- slices) {
      var afterSlice = slice
      if (Pattern.matches("([\\s\\S]*)=~([\\s\\S]*)", slice)) {
        log.info(slice)
        afterSlice = slice.replace("'", "/").replace("%", "([\\s\\S]*)")
        conversionMap += (slice -> afterSlice)
        log.info(afterSlice)
      }
    }

    for (map <- conversionMap) {
      rule = rule.replace(map._1, map._2)
    }

    log.info("The filterRule:" + filterRule + " can be converse to:" + rule)
    rule
  }

  // TODO: add validation with topic entity's fields
  private def getFieldOnFilter(rules: String): mutable.Set[String] = {
    val ruleSlices = rules.trim.split("&&|\\|\\|")
    var fields = mutable.Set[String]()
    var rule: String = null
    var matcher: String = null
    for (slice <- ruleSlices) {
      rule = extractFiledOnStringSlice(slice.trim())
      matcher = isContainsMatcher(rule)
      log.info("The rule slice: " + slice + "response to " + matcher)
      if (matcher == "") {
        throw new Exception("Tracing AviatorFunction.getFieldOnFilter(): Can't parse the rules: "
          + rules + ", the rules maybe exist syntax error at " + rule)
      }
      /* fields.add(rule.split(matcher)(0).trim())*/
      fields += rule.split(matcher)(0).trim()
    }
    log.info("AviatorFunction.getFieldOnFilter() get filter fields: " + fields.toString())
    return fields
  }

  def callGetFiledOnFilter(rules: String): Unit = {
    //    val rules: String = "IP>10.123.1.0 &&  (MAC==' '  || MAC=='unknown') && IP>=10.123.1.0 || IP<=10.123.1.0 && IP =~ a || IP != a"
    getFieldOnFilter(rules)
  }

  private def isContainsMatcher(rule: String): String = {
    if (rule == null) {
      throw new InvalidParameterException("Tracing AviatorFunction.isContainsMatcher():" +
        " Parameter rule should not be null!")
    }
    val matchers = mutable.MutableList[String](">", "<", "==", "!=", "=~")
    var matchSignal: String = ""
    for (matcher <- matchers) {
      if (rule.contains(matcher)) {
        matchSignal = matcher
      }
    }
    matchSignal
  }

  private def extractFiledOnStringSlice(slice: String): String = {
    slice.replace("(", "").replace(")", "")
  }

}
