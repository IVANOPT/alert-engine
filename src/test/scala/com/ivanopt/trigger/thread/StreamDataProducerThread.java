package com.ivanopt.trigger.thread;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ivanopt.repository.ConfigManagementRepository;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.*;
import java.util.Properties;

/**
 * 向kafka中写入测试数据
 *
 * Created with IDEA User: zzzz76 Date: 2018-06-21
 */
public class StreamDataProducerThread extends Thread {
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final String filePath;

  public StreamDataProducerThread(ConfigManagementRepository configManagementRepository,
      String filePath) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configManagementRepository.BROKER_LIST());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        configManagementRepository.KAFKA_KEY_SERIALIZER());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        configManagementRepository.KAFKA_VALUE_SERIALIZER());
    producer = new KafkaProducer<>(props);
    this.topic = configManagementRepository.SOURCE_TOPIC();
    this.filePath = filePath;
  }

  private void sendMessageByLines(String filePath) {
    File file = new File(filePath);
    if (!file.exists() || !file.isFile()) {
      throw new RuntimeException("File is not exist");
    }
    FileInputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader reader = null;
    try {
      fileInputStream = new FileInputStream(file);
      inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
      reader = new BufferedReader(inputStreamReader);
      String lineContent = "";
      while ((lineContent = reader.readLine()) != null) {
        JSONObject lineObject = JSON.parseObject(lineContent);
        if (StringUtils.isEmpty(lineObject.getString("createdTime"))) {
          lineObject.put("createdTime", String.valueOf(System.currentTimeMillis()));
        }
        producer.send(new ProducerRecord<String, String>(topic, lineObject.toJSONString()));
        // System.out.println(record.toJSONString());
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(fileInputStream);
      IOUtils.closeQuietly(inputStreamReader);
      IOUtils.closeQuietly(reader);
    }
  }

  @Override
  public void run() {
    while (true) {
      sendMessageByLines(filePath);
      System.err.println("===================== sleep 5s ======================");
      try {
        sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
