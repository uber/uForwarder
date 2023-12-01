package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.KafkaOffsetCommitterConfiguration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: move kafka offset committer to be part of kafka admin client.

/** KafkaOffsetCommitterFactory is used to create kafka committers. */
public class KafkaOffsetCommitterFactory {

  private static final Logger logger = LoggerFactory.getLogger(KafkaOffsetCommitterFactory.class);
  private final KafkaOffsetCommitterConfiguration config;

  public KafkaOffsetCommitterFactory(KafkaOffsetCommitterConfiguration config) {
    this.config = config;
  }

  /**
   * Creates a {@link KafkaConsumer} used to commit offsets.
   *
   * @param cluster the cluster to consume messages from.
   * @param consumerGroup the consumer group used to consume messages.
   * @param isSecure flag to decide if the client should communicate to broker using TLS and secure
   *     port
   * @return a {@link KafkaConsumer} used to commit offsets.
   * @throws Exception when it fails to create {@link KafkaConsumer}.
   */
  public KafkaConsumer<byte[], byte[]> create(
      String cluster, String consumerGroup, boolean isSecure) throws Exception {
    Properties kafkaConsumerProperties = null;
    try {
      kafkaConsumerProperties = config.getKafkaConsumerProperties(cluster, consumerGroup, isSecure);
    } catch (Exception e) {
      logger.warn(
          "streaming-common.failure",
          StructuredLogging.kafkaCluster(cluster),
          StructuredLogging.kafkaGroup(consumerGroup),
          e);
      throw e;
    }
    return new KafkaConsumer<>(kafkaConsumerProperties);
  }
}
