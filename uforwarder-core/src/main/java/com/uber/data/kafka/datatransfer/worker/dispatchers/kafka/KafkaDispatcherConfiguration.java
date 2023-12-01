package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("worker.dispatcher.kafka")
public class KafkaDispatcherConfiguration {

  private static String ACKS_ONE_CONFIG = "1";
  private static String ACKS_ALL_CONFIG = "all";
  private String resolverClass = KafkaClusterResolver.class.getName();
  private long metricsInterval = 60000; // 1 min
  private long flushInterval = 60000; // 1 min

  private String bootstrapServers = "localhost:9092";
  private String keySerializerClass = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private String valueSerializerClass = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private int maxRequestSize = 1048576;
  private String compressionType = "snappy";

  public Properties getProperties(
      String cluster, String clientId, boolean isSecure, boolean acksOneProducer) throws Exception {
    KafkaClusterResolver clusterResolver = KafkaUtils.newResolverByClassName(resolverClass);
    Properties properties = new Properties();
    properties.setProperty(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        clusterResolver.getBootstrapServers(cluster, bootstrapServers, isSecure));
    properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    if (acksOneProducer) {
      properties.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_ONE_CONFIG);
    } else {
      // use default value, which is acks = all
      properties.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_ALL_CONFIG);
    }

    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
    properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(maxRequestSize));
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    if (isSecure) {
      for (Map.Entry<String, String> entry : KafkaUtils.getSecurityConfigs().entrySet()) {
        properties.setProperty(entry.getKey(), entry.getValue());
      }
    }
    return properties;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getKeySerializerClass() {
    return keySerializerClass;
  }

  public void setKeySerializerClass(String keySerializerClass) {
    this.keySerializerClass = keySerializerClass;
  }

  public String getValueSerializerClass() {
    return valueSerializerClass;
  }

  public void setValueSerializerClass(String valueSerializerClass) {
    this.valueSerializerClass = valueSerializerClass;
  }

  public String getResolverClass() {
    return resolverClass;
  }

  public void setResolverClass(String resolverClass) {
    this.resolverClass = resolverClass;
  }

  public long getMetricsInterval() {
    return metricsInterval;
  }

  public void setMetricsInterval(long metricsInterval) {
    this.metricsInterval = metricsInterval;
  }

  public int getMaxRequestSize() {
    return maxRequestSize;
  }

  public void setMaxRequestSize(int maxRequestSize) {
    this.maxRequestSize = maxRequestSize;
  }

  public String getCompressionType() {
    return this.compressionType;
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
  }

  public long getFlushInterval() {
    return flushInterval;
  }

  public void setFlushInterval(long flushInterval) {
    this.flushInterval = flushInterval;
  }
}
