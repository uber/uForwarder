package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaAdminClientConfigurationTest1 extends FievelTestBase {
  private KafkaAdminClientConfiguration kafkaAdminClientConfiguration;

  @Before
  public void setUp() {
    kafkaAdminClientConfiguration = new KafkaAdminClientConfiguration();
  }

  @Test
  public void testGet() throws Exception {
    Assert.assertEquals("localhost:9092", kafkaAdminClientConfiguration.getBootstrapServers());
    Assert.assertEquals(
        "localhost:9092", kafkaAdminClientConfiguration.getBootstrapServer("cluster"));
    Assert.assertEquals("kafka-consumer-proxy-admin0", kafkaAdminClientConfiguration.getClientId());
    Assert.assertEquals(
        KafkaClusterResolver.class.getName(), kafkaAdminClientConfiguration.getResolverClass());
    Properties properties = kafkaAdminClientConfiguration.getProperties("cluster");
    Assert.assertEquals(
        "kafka-consumer-proxy-admin1",
        properties.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));
    Assert.assertEquals(
        "localhost:9092", properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
  }

  @Test
  public void testSet() {
    kafkaAdminClientConfiguration.setBootstrapServers("127.0.0.1");
    Assert.assertEquals("127.0.0.1", kafkaAdminClientConfiguration.getBootstrapServers());
    kafkaAdminClientConfiguration.setClientId("client-id");
    Assert.assertEquals("client-id0", kafkaAdminClientConfiguration.getClientId());
    kafkaAdminClientConfiguration.setResolverClass("class1");
    Assert.assertEquals("class1", kafkaAdminClientConfiguration.getResolverClass());
  }
}
