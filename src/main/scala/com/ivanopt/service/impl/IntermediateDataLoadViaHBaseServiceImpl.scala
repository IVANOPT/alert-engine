package com.ivanopt.service.impl

import com.alibaba.fastjson.JSONObject
import com.ivanopt.constant.{AlertQuotaConstant, IntermediateDataConstant}
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.IntermediateDataLoadService
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by cong.yu on 18-6-20.
  */
@SerialVersionUID(100L)
class IntermediateDataLoadViaHBaseServiceImpl(configManagementRepository: ConfigManagementRepository, sparkContext: SparkContext)
  extends IntermediateDataLoadService with Serializable {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[IntermediateDataLoadViaHBaseServiceImpl])

  protected def retrieveHBaseTableName(): String = {
    return configManagementRepository.HBASE_INTERMEDIATE_TABLE
  }

  override def loadData(rule: JSONObject, timeRange: (String, String)): RDD[(String, String)] = {
    log.info("IntermediateDataLoadViaHBaseServiceImpl.loadData() | rule: ".concat(rule.toString))
    log.info("IntermediateDataLoadViaHBaseServiceImpl.loadData() | tableName: ".concat(retrieveHBaseTableName()))
    log.info("IntermediateDataLoadViaHBaseServiceImpl.loadData() | timeRange: "
      .concat(timeRange._1).concat(" to ").concat(timeRange._2))

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, retrieveHBaseTableName())
    conf.set(TableInputFormat.SCAN_COLUMNS,
      IntermediateDataConstant.COLUMN_FAMILY.concat(IntermediateDataConstant.COLUMN_FAMILY_DELIMIT)
        .concat(rule.get(AlertQuotaConstant.STATS_TYPE).toString))

    val preRowKey = rule.get(AlertQuotaConstant.ALERT_ID).toString.concat(IntermediateDataConstant.ROWKEY_DELIMIT)
    log.info("IntermediateDataLoadViaHBaseServiceImpl.loadData() | preRowKey: ".concat(preRowKey))

    conf.set(IntermediateDataConstant.HBASE_MAPREDUCE_SCAN_ROW_START, preRowKey.concat(timeRange._1))
    conf.set(IntermediateDataConstant.HBASE_MAPREDUCE_SCAN_ROW_STOP, preRowKey.concat(timeRange._2))
    val hBaseRDD = sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    log.info("IntermediateDataLoadViaHBaseServiceImpl.loadData() | hBaseRDD count: ".concat(hBaseRDD.count().toString))

    hBaseRDD.map(_.toString()).collect().foreach(println)

    // transform (ImmutableBytesWritable, Result) tuples into an RDD of Result’s
    val resultRDD: RDD[org.apache.hadoop.hbase.client.Result] = hBaseRDD.map(tuple => tuple._2)
    // transform into an RDD of (RowKey, ColumnValue)s  the RowKey has the time removed
    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()), Bytes.toString(result.value)))
    keyValueRDD.collect().foreach(kv => println(kv))
    return keyValueRDD
  }

}
