package com.ivanopt.repository

import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.IntermediateDataLoadService
import com.ivanopt.service.impl.{IntermediateDataLoadViaHBaseServiceImpl, IntermediateSuppliedDataLoadViaHBaseServiceImpl}
import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by Ivan on 30/06/2018.
  */
@SerialVersionUID(100L)
class IntermediateDataLoadSelectorRepository extends Serializable {

  private var existDataLoad: mutable.HashMap[String, IntermediateDataLoadService] =
    new mutable.HashMap[String, IntermediateDataLoadService]()
  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository

  existDataLoad.put(AlertQuotaConstant.DISTINCT,
    new IntermediateSuppliedDataLoadViaHBaseServiceImpl(configManagementRepository, SparkContext.getOrCreate()))

  existDataLoad.put(AlertQuotaConstant.DISTINCT_STORE,
    new IntermediateSuppliedDataLoadViaHBaseServiceImpl(configManagementRepository, SparkContext.getOrCreate()))

  def select(key: String): IntermediateDataLoadService = {
    return existDataLoad.getOrElse(key, new IntermediateDataLoadViaHBaseServiceImpl(configManagementRepository, SparkContext.getOrCreate()))
  }

}
