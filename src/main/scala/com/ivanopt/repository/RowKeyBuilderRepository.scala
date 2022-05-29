package com.ivanopt.repository

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.AlertQuotaConstant

import scala.collection.mutable

/**
  * Created by Ivan on 27/06/2018.
  */
@SerialVersionUID(100L)
class RowKeyBuilderRepository(config: JSONObject) extends Serializable {

  private var rowKeyConstructors: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  rowKeyConstructors.put(AlertQuotaConstant.DISTINCT,  retrieveConfigId().concat("_").concat(retrieveCurrentDate())
    .concat("_").concat(retrieveDistinctField()))

  rowKeyConstructors.put(AlertQuotaConstant.DISTINCT_STORE,  retrieveConfigId().concat("_").concat(retrieveCurrentDate())
    .concat("_").concat(retrieveDistinctField()))

  def select(key: String): String = {
    return rowKeyConstructors.getOrElse(key, retrieveConfigId().concat("_").concat(retrieveCurrentDate()))
  }

  private def retrieveConfigId(): String = {
    return String.valueOf(config.get(AlertQuotaConstant.ALERT_ID))
  }

  private def retrieveCurrentDate(): String = {
    return new SimpleDateFormat("YYYYMMddHHmm").format(Calendar.getInstance().getTime())
  }

  private def retrieveDistinctField(): String = {
    return String.valueOf(config.get(AlertQuotaConstant.DISTINCT_FIELD))
  }

}
