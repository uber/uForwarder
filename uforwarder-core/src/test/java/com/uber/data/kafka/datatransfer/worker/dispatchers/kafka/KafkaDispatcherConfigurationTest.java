package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaDispatcherConfigurationTest {
  private KafkaDispatcherConfiguration kafkaDispatcherConfiguration;

  @BeforeEach
  public void setup() {
    kafkaDispatcherConfiguration = new KafkaDispatcherConfiguration();
  }

  @Test
  public void testGetProperties() throws Exception {
    Properties properties =
        kafkaDispatcherConfiguration.getProperties("test-cluster", "test-client", true, true);
    Assertions.assertTrue(properties.containsKey(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assertions.assertEquals("1", properties.getProperty(ProducerConfig.ACKS_CONFIG));

    properties =
        kafkaDispatcherConfiguration.getProperties("test-cluster", "test-client", false, false);
    Assertions.assertFalse(properties.containsKey(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assertions.assertEquals("all", properties.getProperty(ProducerConfig.ACKS_CONFIG));
  }
}
