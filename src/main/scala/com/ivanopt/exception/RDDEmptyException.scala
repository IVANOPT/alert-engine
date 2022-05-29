package com.ivanopt.exception

/**
  * Created by cong.yu on 18-6-22.
  */
//TODO: extend runtime exception to avoid interruption
class RDDEmptyException(msg: String, cause: Throwable) extends Exception(msg: String, cause: Throwable) {
  def this(msg: String) {
    this(msg, null)
  }
}
