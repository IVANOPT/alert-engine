package com.ivanopt.repository

import com.ivanopt.constant.AlertQuotaConstant
import com.ivanopt.service.IntermediateDataStoreService
import com.ivanopt.service.impl.{IntermediateDataStoreViaHBaseServiceImpl, IntermediateSuppliedDataStoreViaHBaseServiceImpl}

import scala.collection.mutable

/**
  * Created by Ivan on 27/06/2018.
  */
@SerialVersionUID(100L)
class IntermediateDataStoreSelectorRepository extends Serializable {

  private var existDataStore: mutable.HashMap[String, IntermediateDataStoreService] =
    new mutable.HashMap[String, IntermediateDataStoreService]()
  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository

  existDataStore.put(AlertQuotaConstant.DISTINCT, new IntermediateSuppliedDataStoreViaHBaseServiceImpl(configManagementRepository))
  existDataStore.put(AlertQuotaConstant.DISTINCT_STORE, new IntermediateSuppliedDataStoreViaHBaseServiceImpl(configManagementRepository))

  def select(key: String): IntermediateDataStoreService = {
    return existDataStore.getOrElse(key, new IntermediateDataStoreViaHBaseServiceImpl(configManagementRepository))
  }

}