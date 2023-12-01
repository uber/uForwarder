package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "master.kafka.offsetcommitter")
public class KafkaOffsetCommitterConfiguration {

  private static final String CLIENT_ID_PREFIX = "kafka-consumer-proxy-offset-committer";
  private static final String KEY_DESERIALIZER_CLASS =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  private static final String VALUE_DESERIALIZER_CLASS =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  // should disable the auto commit
  private static final String ENABLE_AUTO_COMMIT = "false";
  // add a suffix to client ID so each Kafka consumer has a unique client ID
  private final AtomicInteger CLIENT_ID_SUFFIX = new AtomicInteger(0);

  // The follow configurations are used outside of the OffsetFetcherConsumer
  private String resolverClass = KafkaClusterResolver.class.getName();
  // The following configurations are passed through to OffsetFetcherConsumer constructor.
  private String bootstrapServers = "localhost:9092";

  public String getResolverClass() {
    return resolverClass;
  }

  public void setResolverClass(String resolverClass) {
    this.resolverClass = resolverClass;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  /**
   * Gets the {@link Properties} used to create kafka consumers.
   *
   * @param cluster the cluster to consume messages from.
   * @param consumerGroup the consumer group used to consume messages.
   * @param isSecure flag to decide if the client should communicate to broker using TLS and secure
   *     port
   * @return the {@link Properties} used to create kafka consumers.
   * @throws Exception when it fails to create {@link Properties}.
   */
  public Properties getKafkaConsumerProperties(
      String cluster, String consumerGroup, boolean isSecure) throws Exception {
    KafkaClusterResolver clusterResolver = KafkaUtils.newResolverByClassName(resolverClass);
    Properties properties = new Properties();
    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        clusterResolver.getBootstrapServers(cluster, bootstrapServers, isSecure));
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.setProperty(
        ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + CLIENT_ID_SUFFIX.getAndIncrement());
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS);
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
    if (isSecure) {
      for (Map.Entry<String, String> entry : KafkaUtils.getSecurityConfigs().entrySet()) {
        properties.setProperty(entry.getKey(), entry.getValue());
      }
    }
    return properties;
  }
}
