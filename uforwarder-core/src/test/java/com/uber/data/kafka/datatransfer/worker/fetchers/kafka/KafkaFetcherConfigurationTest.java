package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Assert;
import org.junit.Test;

public class KafkaFetcherConfigurationTest extends FievelTestBase {
  private KafkaFetcherConfiguration kafkaFetcherConfiguration = new KafkaFetcherConfiguration();

  private static class TestResolver extends KafkaClusterResolver {}

  @Test
  public void testGet() throws Exception {
    Assert.assertEquals(1, kafkaFetcherConfiguration.getNumberOfFetchers());
    Assert.assertEquals(1000, kafkaFetcherConfiguration.getOffsetCommitIntervalMs());
    Assert.assertEquals(1000, kafkaFetcherConfiguration.getOffsetMonitorIntervalMs());
    Assert.assertEquals(100, kafkaFetcherConfiguration.getPollTimeoutMs());
    Assert.assertEquals(
        "com.uber.data.kafka.datatransfer.common.KafkaClusterResolver",
        kafkaFetcherConfiguration.getResolverClass());
    Properties properties =
        kafkaFetcherConfiguration.getKafkaConsumerProperties(
            "localhost:9092", "client-id", "group", IsolationLevel.ISOLATION_LEVEL_UNSET, false);
    Assert.assertEquals("client-id", properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    Assert.assertEquals(
        "earliest", properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals("group", properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assert.assertEquals("false", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    Assert.assertEquals(
        "localhost:9092", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertNull(properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    Assert.assertNull(properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assert.assertNull(properties.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assert.assertNull(properties.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertNull(properties.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
  }

  @Test
  public void testSet() throws Exception {
    kafkaFetcherConfiguration.setBootstrapServers("127.0.0.1");
    kafkaFetcherConfiguration.setNumberOfFetchers(2);
    kafkaFetcherConfiguration.setOffsetCommitIntervalMs(1);
    kafkaFetcherConfiguration.setOffsetMonitorIntervalMs(1);
    kafkaFetcherConfiguration.setPollTimeoutMs(1);
    kafkaFetcherConfiguration.setResolverClass(TestResolver.class.getName());
    kafkaFetcherConfiguration.setCommitOnIdleFetcher(true);
    Assert.assertEquals(2, kafkaFetcherConfiguration.getNumberOfFetchers());
    Assert.assertEquals(1, kafkaFetcherConfiguration.getOffsetCommitIntervalMs());
    Assert.assertEquals(1, kafkaFetcherConfiguration.getOffsetMonitorIntervalMs());
    Assert.assertEquals(1, kafkaFetcherConfiguration.getPollTimeoutMs());
    Assert.assertEquals(TestResolver.class.getName(), kafkaFetcherConfiguration.getResolverClass());
    Assert.assertEquals(true, kafkaFetcherConfiguration.getCommitOnIdleFetcher());
    kafkaFetcherConfiguration.setResolverClass(KafkaClusterResolver.class.getName());
    Properties properties =
        kafkaFetcherConfiguration.getKafkaConsumerProperties(
            "127.0.0.1",
            "client-id",
            "group",
            AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST,
            IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED,
            true);
    Assert.assertEquals("client-id", properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    Assert.assertEquals("latest", properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        properties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals("group", properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assert.assertEquals("false", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    Assert.assertEquals(
        "127.0.0.1", properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals(
        "read_committed", properties.getProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SECURITY_PROTOCOL,
        properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        properties.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        properties.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SECURITY_PROVIDER_CLASS,
        properties.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
  }
}
