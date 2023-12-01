package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaDispatcherConfigurationTest extends FievelTestBase {
  private KafkaDispatcherConfiguration kafkaDispatcherConfiguration;

  @Before
  public void setup() {
    kafkaDispatcherConfiguration = new KafkaDispatcherConfiguration();
  }

  @Test
  public void testGetProperties() throws Exception {
    Properties properties =
        kafkaDispatcherConfiguration.getProperties("test-cluster", "test-client", true, true);
    Assert.assertTrue(properties.containsKey(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals("1", properties.getProperty(ProducerConfig.ACKS_CONFIG));

    properties =
        kafkaDispatcherConfiguration.getProperties("test-cluster", "test-client", false, false);
    Assert.assertFalse(properties.containsKey(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals("all", properties.getProperty(ProducerConfig.ACKS_CONFIG));
  }
}
