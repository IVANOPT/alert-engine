package com.ivanopt.service.impl

import com.ivanopt.constant.IntermediateDataConstant
import com.ivanopt.repository.ConfigManagementRepository
import com.ivanopt.service.IntermediateDataStoreService
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

/**
  * Created by Ivan on 08/06/2018.
  */
@SerialVersionUID(100L)
class IntermediateDataStoreViaHBaseServiceImpl(configManagementRepository: ConfigManagementRepository) extends IntermediateDataStoreService {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[IntermediateDataStoreViaHBaseServiceImpl])

  protected def retrieveHBaseTableName(): String = {
    return configManagementRepository.HBASE_INTERMEDIATE_TABLE
  }

  override def storeData(toStoreRDD: RDD[(String, String, String, Any)]): Unit = {
    log.info(s"IntermediateDataStoreViaHBaseServiceImpl.storeData() | HBaseTableName: ".concat(retrieveHBaseTableName()))
    val conf = HBaseConfiguration.create()
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(IntermediateDataConstant.MAPREDUCE_OUTPUT_FILEOUTPUTFORMAT_OUTPUTDIR, configManagementRepository.HBASE_OUTPUT_DIR)
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, retrieveHBaseTableName())
    toStoreRDD.map { case (rowKey, columnFamily, column, value) => prepareToPut(rowKey, columnFamily, column, value) }.saveAsHadoopDataset(jobConfig)
  }

  def prepareToPut(rowKey: String, columnFamily: String, column: String, value: Any): (ImmutableBytesWritable, Put) = {
    val putRecord: Put = new Put(Bytes.toBytes(rowKey))
    putRecord.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(String.valueOf(value)))
    return (new ImmutableBytesWritable, putRecord)
  }

  override protected var configReloadDemand: Boolean = true

}
