package com.ivanopt.utils.aviator

import java.util

import com.alibaba.fastjson.JSON
import com.googlecode.aviator.{AviatorEvaluator, Expression}
import com.ivanopt.bean.AviatorTestBean
import org.testng.annotations.Test

class AviatorTest {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[AviatorTest])

  /*  @BeforeTest
    def generationAviatorFunction(): Unit = {
      AviatorFunction = new AviatorFunction
    }*/

  @Test
  def AddTest(): Unit = {
    val result: Long = AviatorEvaluator.execute("1+2+3").asInstanceOf[Long]
    log.info(result.toString)
  }

  @Test
  def conversionTest(): Unit = {
    val rule: String = "IP>10.123.1.0 AND  (MAC=' '  OR MAC='unknown') " +
      "AND IP>=10.123.1.0 OR IP<=10.123.1.0 IP LIKE a OR IP <> a AND name LIKE '%monkey%'"
    AviatorFunction.conversion(rule)
  }

  @Test
  def callGetFiledOnFilterTest(): Unit = {
    val rule: String = "IP>10.123.1.0 AND  (MAC=' '  OR MAC='unknown') " +
      "AND IP>=10.123.1.0 AND IP<=10.123.1.0 OR IP <> a AND name LIKE '%monkey%'"
    val rules = AviatorFunction.conversion(rule)
    AviatorFunction.callGetFiledOnFilter(rules)
  }

  @Test
  //aviator expression: if the value is String, it must be included in '', otherwise it's a parameter or a number
  def executeTest(): Unit = {
    val rule: String = "IP='10.123.1.0' AND  (MAC=a OR MAC='2'  OR MAC='unknown') AND name LIKE '%monkey%'"
    val rules = AviatorFunction.conversion(rule)
    val entity: AviatorTestBean = new AviatorTestBean("10.123.1.0".asInstanceOf[Object], "2".asInstanceOf[Object], "45monkeys*")
    //    Assert.assertTrue(AviatorFunction.execute(rules, "test"))
  }

  @Test
  def regPatternTest(): Unit = {
    val str: String = "fshid3423f 932d  df\\n"
    val env: java.util.Map[String, Object] = new util.HashMap[String, Object]()
    env.put("testStr", str)
    val outPut: String = AviatorEvaluator.execute("testStr=~/([\\s\\S]*)/ ? $1 : 'unknow'", env).toString
    log.info(outPut)
    val regex: String = " name LIKE '%karry%' "
    val afterRegex: String = regex.replace("LIKE", "=~")
      .replace("'%", "/([\\s\\S]*)").replace("%'", "([\\s\\S]*)/")
    log.info(afterRegex)
    val strReg: String = "38karsryfds"
    env.put("name", strReg)
    val compileExp: Expression = AviatorEvaluator.compile(afterRegex)
    val result: Boolean = compileExp.execute(env).asInstanceOf[Boolean]
    //    val s: String = AviatorEvaluator.execute("afterRegex ? $1 : 'cant' ", env).toString
    log.info(result.toString)

  }

  @Test
  def excuteTest(): Unit = {
    val jsonStr: String = "{\"name\":\"dsdfmonkeyfd\", \"IP\":\"10.123.1.0\", \"MAC\":2, \"sex\":\"male\"}"
    val json = JSON.parseObject(jsonStr)
    val name = json.get("name")
    log.info(name.asInstanceOf[String])
    val rule: String = "IP='10.123.1.0' AND  (MAC=a OR MAC>1  OR MAC='unknown') AND name LIKE '%ds%monkey%' AND sex LIKE 'male'"
    val rules = AviatorFunction.conversion(rule)
    //    Assert.assertTrue(AviatorFunction.execute(rules, jsonStr))
  }

  @Test
  def parternTest(): Unit = {
    import java.util.regex.Pattern
    var original = "IP>10.123.1.0 &&  (MAC==' '  || MAC=='unknown') && IP>=10.123.1.0 && IP<=10.123.1.0 && name =~ 'D%a%d%d' || IP != a"

    var text = "IP>10.123.1.0 &&  (MAC==' '  || MAC=='unknown') && IP>=10.123.1.0 && IP<=10.123.1.0 && name =~ 'D%a%d%d' || IP != a"
    val slices = text.split("&&|\\|\\|")
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
      text = text.replace(map._1, map._2)
    }
    log.info(text)
    log.info(original)
    /*    val patternString = "like\\s*'"
        val pattern = Pattern.compile(patternString)
        val matcher = pattern.matcher(text)
        var count = 0
        while ( {
          matcher.find
        }) {
          count += 1
          System.out.println("found: " + count + " : " + matcher.start + " - " + matcher.end)
        }*/

  }


}
