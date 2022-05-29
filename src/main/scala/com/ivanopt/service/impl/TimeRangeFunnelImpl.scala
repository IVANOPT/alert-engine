package com.ivanopt.service.impl

import java.lang

import com.alibaba.fastjson.JSONObject
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.DataFunnel

/**
  * Created by Ivan on 20/06/2018.
  */
@SerialVersionUID(101L)
class TimeRangeFunnelImpl(configManagementRepository: ConfigManagementRepository) extends DataFunnel {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[TimeRangeFunnelImpl])

  val unitMappingMilliseconds: Map[String, Long] = Map("min" -> 60 * 1000L, "hour" -> 60 * 60 * 1000L, "day" -> 24 * 60 * 60 * 1000L)

  override def condition(): String = {
    return configManagementRepository.BIZ_GENERATION_TIME_FIELD
  }

  override def target(item: JSONObject): lang.Long = {
    log.info("TimeRangeFunnelImpl.target() | item: ".concat(item.toString))
    log.info("TimeRangeFunnelImpl.target() | condition: ".concat(condition()))
    return lang.System.currentTimeMillis() - lang.Long.parseLong(item.get(condition()).asInstanceOf[String])
  }

  override def generationMatched(target: java.lang.Long, threshold: java.lang.Integer, unit: String): Boolean = {
    log.info(s"target: $target, threshold: $threshold, unit: $unit")
    log.info(s"threshold converted: ".concat(String.valueOf(threshold * unitMappingMilliseconds(unit))))
    return target <= threshold * unitMappingMilliseconds(unit)
  }

}
