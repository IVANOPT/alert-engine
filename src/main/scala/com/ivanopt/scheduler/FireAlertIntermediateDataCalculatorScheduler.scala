package com.ivanopt.scheduler

import java.util.{ Timer, TimerTask }

import com.alibaba.fastjson.{ JSON, JSONObject }
import com.ivanopt.constant.{ AlertQuotaConstant, IntermediateDataConstant }
import com.ivanopt.repository.{ ConfigManagementRepository, IntermediateDataLoadSelectorRepository }
import com.ivanopt.service.ScannerService
import com.ivanopt.service.impl.{ AlertIntermediateDataLoadViaHBaseServiceImpl, MessageProducerViaKafkaServiceImpl, SchedulerConfigReloadScannerServiceImpl }
import com.ivanopt.utils.alert.AlertSinkUtils
import org.apache.spark.rdd.RDD

/**
 * Created by Ivan on 08/06/2018.
 */
object FireAlertIntermediateDataCalculatorScheduler extends Scheduler {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(FireAlertIntermediateDataCalculatorScheduler.getClass)

  private val configManagementRepository: ConfigManagementRepository = new ConfigManagementRepository()
  private final val messageProducerViaKafkaServiceImpl = new MessageProducerViaKafkaServiceImpl(configManagementRepository)
  private val sparkContext = initSparkContext()

  private val intermediateDataStoreSelectorService = new IntermediateDataLoadSelectorRepository()

  def alertCollector(alert: JSONObject): Unit = {
    log.info("FireAlertIntermediateDataCalculatorScheduler.alertCollector() | alert: "
      .concat(if (alert == null) "null" else alert.toString))
    val storeRDD: RDD[String] = sparkContext.parallelize(List(alert.toString))
    val storeDs = storeRDD.map(rdd => AlertSinkUtils.convertCollectorSource(JSON.parseObject(rdd)))
    messageProducerViaKafkaServiceImpl.publish(storeDs, configManagementRepository.SINK_TOPIC)
  }

  def main(args: Array[String]): Unit = {

    val alertService = new AlertIntermediateDataLoadViaHBaseServiceImpl().init()
    val timer: Timer = new Timer()
    var scanner: ScannerService[Scheduler] = new SchedulerConfigReloadScannerServiceImpl(timer)
    scanner.add(this).start()

    while (true) {

      if (configReloadDemand) {
        log.info(s"FireAlertCalculatorService.main() | config reloading.")

        val ruleArr = sparkContext.textFile(configManagementRepository.BIZ_ALERT_CONFIG_LOCATION)
          .map(line => JSON.parseObject(line))
          .collect()

        val task: TimerTask = new TimerTask {
          override def run(): Unit = {
            for (rule <- ruleArr) {
              val result = alertService.notifyOb(
                rule,
                intermediateDataStoreSelectorService.
                  select(AlertSinkUtils.statsTypeConversionForStorage(rule).get(AlertQuotaConstant.STATS_TYPE).asInstanceOf[String]))
              println(result)
              if (null != result) {
                alertCollector(result)
              }
            }
          }
        }
        task.run()

        configReloadDemand = false
        timer.schedule(task, IntermediateDataConstant.DELAY, IntermediateDataConstant.PERIOD)

      }

    }

  }

}