package com.ivanopt.utils.alert

import com.alibaba.fastjson.JSON
import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by cong.yu on 18-6-26.
  */
class AlertUtilsTest {
  @Test
  def execute(): Unit = {
    val result = JSON.parseObject("{\"baselineMax\":1.5,\"alertType\":\"BaselineStatsAlert\",\"statsType\":\"baseline\",\"description\":\"Alert—name1大于1000\",\"threshold\":1000,\"baselineTimeRange\":\"yesterday\",\"fieldValue\":\"400\",\"filterInfo\":\"mac = ''\",\"baselineMin\":0.5,\"validationTime\":\"2018-05-30 00:00:00\",\"sourceEntity\":\"securities\",\"compareType\":\"gt\",\"baselineValue\":10.0,\"statsField\":\"name\",\"timeNum\":500,\"name\":\"客户端频繁请求\",\"id\":2,\"baselineSection\":\"left\",\"alertTime\":\"2018-06-26 17:13:05\",\"finalValue\":8.333333333333334,\"timeUnit\":\"min\",\"status\":\"WAITING\"}")
    val rule = JSON.parseObject("{\"id\":2, \"name\":\"客户端频繁请求\", \"description\":\"Alert—name1大于1000\", \"sourceEntity\":\"securities\", \"filterInfo\":\"mac = ''\", \"alertType\":\"EventCountAlert\", \"timeNum\":500, \"timeUnit\":\"min\", \"statsType\":\"baseline\", \"statsField\":\"name\", \"compareType\":\"gt\", \"threshold\":1000, \"fieldValue\":\"400\", \"baselineTimeRange\":\"yesterday\", \"baselineMin\":0.5, \"baselineMax\":1.5, \"baselineSection\":\"middle\", \"validationTime\":\"2018-05-30 00:00:00\", \"status\":\"WAITING\" }")
    Assert.assertTrue(AlertUtils.execute(result, rule))
  }

}
