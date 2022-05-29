package com.ivanopt.service.impl

import com.ivanopt.repository.ConfigManagementRepository

/**
  * Created by Ivan on 27/06/2018.
  */
@SerialVersionUID(101L)
class IntermediateSuppliedDataStoreViaHBaseServiceImpl(configManagementRepository: ConfigManagementRepository)
  extends IntermediateDataStoreViaHBaseServiceImpl(configManagementRepository) {

  override protected def retrieveHBaseTableName(): String = {
    return configManagementRepository.HBASE_INTERMEDIATE_SUPPLIED_TABLE
  }

}
