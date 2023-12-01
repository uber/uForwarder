package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for KafkaFetcher. */
@ConfigurationProperties("worker.fetcher.kafka")
public final class KafkaFetcherConfiguration {

  private static final String KEY_DESERIALIZER_CLASS =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  private static final String VALUE_DESERIALIZER_CLASS =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  // should disable the auto commit
  private static final String ENABLE_AUTO_COMMIT = "false";

  // some topics has max message size configured 10MB, to avoid block consumer, bump up consumer
  // limit as well
  private static final String DEFAULT_MAX_PARTITION_FETCH_BYTES_CONFIG = "10485760";

  // we saw session timeout with default 10s setting, bump this setting to tolerant higher latency
  private static final String DEFAULT_SESSION_TIMEOUT_MS_CONFIG = "30000";

  // Determines whether to use configuration or streaming common to resolve brokers.
  private String resolverClass = KafkaClusterResolver.class.getName();
  // The following configuration should be passed to Kafka Consumer
  private String autoOffsetResetPolicy = "earliest"; // to guarantee no data loss
  private String bootstrapServers = "localhost:9092";

  // The following are internal to the fetcher.
  private int pollTimeoutMs = 100;
  private int numberOfFetchers = 1;
  private int offsetCommitIntervalMs = 1000;
  private int offsetMonitorIntervalMs = 1000;

  private boolean commitOnIdleFetcher = false;

  public Properties getKafkaConsumerProperties(
      String bootstrapServers,
      String clientId,
      String consumerGroup,
      IsolationLevel isolationLevel,
      boolean isSecure) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetPolicy);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS);
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
    properties.setProperty(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, DEFAULT_MAX_PARTITION_FETCH_BYTES_CONFIG);
    properties.setProperty(
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, DEFAULT_SESSION_TIMEOUT_MS_CONFIG);
    // READ_UNCOMMITTED is considered default value for KafkaConsumer
    if (isolationLevel == IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED) {
      properties.setProperty(
          ConsumerConfig.ISOLATION_LEVEL_CONFIG,
          org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED
              .toString()
              .toLowerCase(Locale.ROOT));
    }
    if (isSecure) {
      for (Map.Entry<String, String> entry : KafkaUtils.getSecurityConfigs().entrySet()) {
        properties.setProperty(entry.getKey(), entry.getValue());
      }
    }
    return properties;
  }

  public String getBootstrapServers() {
    return this.bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public int getPollTimeoutMs() {
    return pollTimeoutMs;
  }

  public void setPollTimeoutMs(int pollTimeoutMs) {
    this.pollTimeoutMs = pollTimeoutMs;
  }

  public int getNumberOfFetchers() {
    return numberOfFetchers;
  }

  public void setNumberOfFetchers(int numberOfFetchers) {
    this.numberOfFetchers = numberOfFetchers;
  }

  public void setResolverClass(String resolverClass) {
    this.resolverClass = resolverClass;
  }

  public void setAutoOffsetResetPolicy(String autoOffsetRestPolicy) {
    this.autoOffsetResetPolicy = autoOffsetRestPolicy;
  }

  public String getResolverClass() {
    return resolverClass;
  }

  public String getAutoOffsetResetPolicy() {
    return autoOffsetResetPolicy;
  }

  public int getOffsetCommitIntervalMs() {
    return offsetCommitIntervalMs;
  }

  public void setOffsetCommitIntervalMs(int offsetCommitIntervalMs) {
    this.offsetCommitIntervalMs = offsetCommitIntervalMs;
  }

  public int getOffsetMonitorIntervalMs() {
    return offsetMonitorIntervalMs;
  }

  public void setOffsetMonitorIntervalMs(int offsetMonitorIntervalMs) {
    this.offsetMonitorIntervalMs = offsetMonitorIntervalMs;
  }

  public boolean getCommitOnIdleFetcher() {
    return commitOnIdleFetcher;
  }

  public void setCommitOnIdleFetcher(Boolean commitOnIdleFetcher) {
    this.commitOnIdleFetcher = commitOnIdleFetcher;
  }
}
