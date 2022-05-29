package com.ivanopt.service.impl

import org.testng.annotations.Test

class ConfigReloadScannerServiceImplTest {

  @Test(groups = Array("scanner-service"))
  def scanRequiredTest(): Unit = {
    assert(new StreamingDataConfigReloadScannerServiceImpl(null).scanRequired("201806211507") == false)
  }

}
