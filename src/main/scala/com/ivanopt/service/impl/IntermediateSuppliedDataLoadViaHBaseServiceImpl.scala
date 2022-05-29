package com.ivanopt.service.impl

import com.ivanopt.repository.ConfigManagementRepository
import org.apache.spark.SparkContext

/**
  * Created by Ivan on 30/06/2018.
  */
@SerialVersionUID(100L)
class IntermediateSuppliedDataLoadViaHBaseServiceImpl(configManagementRepository: ConfigManagementRepository, sparkContext: SparkContext)
  extends IntermediateDataLoadViaHBaseServiceImpl(configManagementRepository, sparkContext) with Serializable {

  override protected def retrieveHBaseTableName(): String = {
    return configManagementRepository.HBASE_INTERMEDIATE_SUPPLIED_TABLE
  }

}
