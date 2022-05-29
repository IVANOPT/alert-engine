package com.ivanopt.repository

import java.io._
import java.nio.file.Paths
import java.util.Properties

/**
  * Created by Ivan on 07/05/2017.
  */
@SerialVersionUID(101L)
class ConfigManagementRepository extends Serializable {

  private final val log: org.slf4j.Logger = org.slf4j.LoggerFactory.getLogger(classOf[ConfigManagementRepository])

  final val ZK_LIST: String = loadProperties("kafka.zk_list")
  final val ZK_PATH: String = loadProperties("kafka.zk_path")
  final val BROKER_LIST: String = loadProperties("kafka.broker_list")
  final val SOURCE_TOPIC: String = loadProperties("kafka.topic.source")
  final val SINK_TOPIC: String = loadProperties("kafka.topic.sink")
  final val GROUP_ID: String = loadProperties("kafka.group_id")
  final val KAFKA_KEY_SERIALIZER = loadProperties("kafka.key.serializer")
  final val KAFKA_VALUE_SERIALIZER = loadProperties("kafka.value.serializer")

  final val HBASE_INTERMEDIATE_TABLE = loadProperties("hbase.intermediate.table")
  final val HBASE_INTERMEDIATE_SUPPLIED_TABLE = loadProperties("hbase.intermediate.supplied.table")
  final val HBASE_OUTPUT_DIR = loadProperties("hbase.output.dir")

  final val REDIS_SERVER_LIST: String = loadProperties("redis.server_list")

  final val BIZ_GENERATION_TIME_FIELD: String = loadProperties("biz.generation.time.field")
  final val BIZ_ALERT_CONFIG_LOCATION: String = loadProperties("biz.alert.config.location")

  private final val GSS_JAAS_CONF_PATH: String = loadProperties("gss_jaas_conf_path")
  private final val KRB5_CONF_PATH: String = loadProperties("krb5_conf_path")

  private def loadProperties(key: String): String = {
    val currentPath: String = Paths.get(".").toAbsolutePath().normalize().toString()
    log.info("ConfigManagementRepository.loadProperties | current path: ".concat(currentPath))
    val fileDir: File = new File("application.properties")

    val bufferedReaderIn: BufferedReader =
      new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"))

    val prop: Properties = new Properties()
    try {
      prop.load(bufferedReaderIn)
      return prop.getProperty(key)
    } catch {
      case e: Exception => log.info(e.toString(), "could not find application.properties")
    }

    return ""
  }

  def loadSystemProps(): Unit = {
    var sb: StringBuilder = new StringBuilder
    sb.append("Using krb5.conf: ").append(KRB5_CONF_PATH)
    log.info(sb.toString())
    sb.setLength(0)
    sb.append("Using gss-jaas.conf: ").append(GSS_JAAS_CONF_PATH)
    log.info(sb.toString())

    log.info("System begins to load kerberos configuration...")
    System.setProperty("java.security.auth.login.config", GSS_JAAS_CONF_PATH)
    System.setProperty("sun.security.jgss.debug", "true")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH)
    log.info(System.getProperties.toString)
  }

}

