package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.consumerproxy.config.KafkaOffsetCommitterConfiguration;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class KafkaOffsetCommitterFactoryTest extends FievelTestBase {
  @Test
  public void testCreateKafkaOffsetCommitter() {
    KafkaOffsetCommitterConfiguration kafkaOffsetCommitterConfiguration =
        new KafkaOffsetCommitterConfiguration();
    KafkaOffsetCommitterFactory kafkaOffsetCommitterFactory =
        new KafkaOffsetCommitterFactory(kafkaOffsetCommitterConfiguration);
    Exception exception = null;
    KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
    try {
      kafkaConsumer = kafkaOffsetCommitterFactory.create("cluster", "consumerGroup", false);
    } catch (Exception e) {
      exception = e;
    }
    Assert.assertNull(exception);
    Assert.assertNotNull(kafkaConsumer);
  }

  @Test
  public void testCreateKafkaOffsetCommitterWithException() throws Exception {
    KafkaOffsetCommitterConfiguration kafkaOffsetCommitterConfiguration =
        Mockito.mock(KafkaOffsetCommitterConfiguration.class);
    KafkaOffsetCommitterFactory kafkaOffsetCommitterFactory =
        new KafkaOffsetCommitterFactory(kafkaOffsetCommitterConfiguration);
    Mockito.when(
            kafkaOffsetCommitterConfiguration.getKafkaConsumerProperties(
                "cluster", "consumerGroup", false))
        .thenThrow(new Exception());
    Exception exception = null;
    KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
    try {
      kafkaConsumer = kafkaOffsetCommitterFactory.create("cluster", "consumerGroup", false);
    } catch (Exception e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
    Assert.assertNull(kafkaConsumer);
  }
}
