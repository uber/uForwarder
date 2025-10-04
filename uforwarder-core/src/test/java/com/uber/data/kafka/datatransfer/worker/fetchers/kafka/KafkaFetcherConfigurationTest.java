package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaFetcherConfigurationTest {
  private KafkaFetcherConfiguration kafkaFetcherConfiguration = new KafkaFetcherConfiguration();

  private static class TestResolver extends KafkaClusterResolver {}

  @Test
  public void testGet() throws Exception {
    Assertions.assertEquals(1, kafkaFetcherConfiguration.getNumberOfFetchers());
    Assertions.assertEquals(1000, kafkaFetcherConfiguration.getOffsetCommitIntervalMs());
    Assertions.assertEquals(1000, kafkaFetcherConfiguration.getOffsetMonitorIntervalMs());
    Assertions.assertEquals(100, kafkaFetcherConfiguration.getPollTimeoutMs());
    Assertions.assertEquals(
        "com.uber.data.kafka.datatransfer.common.KafkaClusterResolver",
        kafkaFetcherConfiguration.getResolverClass());
    Properties properties =
        kafkaFetcherConfiguration.getKafkaConsumerProperties(
            "localhost:9092", "client-id", "group", IsolationLevel.ISOLATION_LEVEL_UNSET, false);
    Assertions.assertEquals("client-id", properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    Assertions.assertEquals(
        "earliest", properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    Assertions.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assertions.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    Assertions.assertEquals("group", properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assertions.assertEquals(
        "false", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    Assertions.assertEquals(
        "localhost:9092", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assertions.assertNull(properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    Assertions.assertNull(properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assertions.assertNull(properties.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assertions.assertNull(properties.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assertions.assertNull(properties.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
  }

  @Test
  public void testSet() throws Exception {
    kafkaFetcherConfiguration.setBootstrapServers("127.0.0.1");
    kafkaFetcherConfiguration.setNumberOfFetchers(2);
    kafkaFetcherConfiguration.setOffsetCommitIntervalMs(1);
    kafkaFetcherConfiguration.setOffsetMonitorIntervalMs(1);
    kafkaFetcherConfiguration.setPollTimeoutMs(1);
    kafkaFetcherConfiguration.setResolverClass(TestResolver.class.getName());
    Assertions.assertEquals(2, kafkaFetcherConfiguration.getNumberOfFetchers());
    Assertions.assertEquals(1, kafkaFetcherConfiguration.getOffsetCommitIntervalMs());
    Assertions.assertEquals(1, kafkaFetcherConfiguration.getOffsetMonitorIntervalMs());
    Assertions.assertEquals(1, kafkaFetcherConfiguration.getPollTimeoutMs());
    Assertions.assertEquals(
        TestResolver.class.getName(), kafkaFetcherConfiguration.getResolverClass());
    kafkaFetcherConfiguration.setResolverClass(KafkaClusterResolver.class.getName());
    Properties properties =
        kafkaFetcherConfiguration.getKafkaConsumerProperties(
            "127.0.0.1",
            "client-id",
            "group",
            AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST,
            IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED,
            true);
    Assertions.assertEquals("client-id", properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    Assertions.assertEquals(
        "latest", properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    Assertions.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assertions.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    Assertions.assertEquals("group", properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assertions.assertEquals(
        "false", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    Assertions.assertEquals(
        "127.0.0.1", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assertions.assertEquals(
        "read_committed", properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    Assertions.assertEquals(
        KafkaUtils.SECURITY_PROTOCOL,
        properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assertions.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        properties.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assertions.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        properties.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assertions.assertEquals(
        KafkaUtils.SECURITY_PROVIDER_CLASS,
        properties.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
  }
}
