package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaAdminClientConfigurationTest1 {
  private KafkaAdminClientConfiguration kafkaAdminClientConfiguration;

  @BeforeEach
  public void setUp() {
    kafkaAdminClientConfiguration = new KafkaAdminClientConfiguration();
  }

  @Test
  public void testGet() throws Exception {
    Assertions.assertEquals("localhost:9092", kafkaAdminClientConfiguration.getBootstrapServers());
    Assertions.assertEquals(
        "localhost:9092", kafkaAdminClientConfiguration.getBootstrapServer("cluster"));
    Assertions.assertEquals(
        "kafka-consumer-proxy-admin0", kafkaAdminClientConfiguration.getClientId());
    Assertions.assertEquals(
        KafkaClusterResolver.class.getName(), kafkaAdminClientConfiguration.getResolverClass());
    Properties properties = kafkaAdminClientConfiguration.getProperties("cluster");
    Assertions.assertEquals(
        "kafka-consumer-proxy-admin1",
        properties.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));
    Assertions.assertEquals(
        "localhost:9092", properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    Assertions.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assertions.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
  }

  @Test
  public void testSet() {
    kafkaAdminClientConfiguration.setBootstrapServers("127.0.0.1");
    Assertions.assertEquals("127.0.0.1", kafkaAdminClientConfiguration.getBootstrapServers());
    kafkaAdminClientConfiguration.setClientId("client-id");
    Assertions.assertEquals("client-id0", kafkaAdminClientConfiguration.getClientId());
    kafkaAdminClientConfiguration.setResolverClass("class1");
    Assertions.assertEquals("class1", kafkaAdminClientConfiguration.getResolverClass());
  }
}
