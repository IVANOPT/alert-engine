package com.ivanopt.constant

/**
  * Created by cong.yu on 15/06/2018.
  */
object TimeConstant {
  final val TIME_MAP: Map[String, Long] = Map(
    "sec" -> 1, "min" -> 60, "hour" -> 60 * 60, "day" -> 24 * 60 * 60
  )

  final val SEC = "sec"
  final val MIN = "min"
  final val HOUR = "hour"
  final val DAY = "day"

  final val SLIDE_DURATION: Long = 5
}
