package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
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
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {KafkaOffsetCommitterConfiguration.class})
@TestPropertySource(
  properties = {"spring.config.location=classpath:/static-mode-offset-committer.yaml"}
)
public class KafkaOffsetCommitterConfigurationTest2 extends FievelTestBase {

  @Autowired private KafkaOffsetCommitterConfiguration kafkaOffsetCommitterConfiguration;

  @Test
  public void testWithoutSecurity() throws Exception {
    Assert.assertEquals(
        KafkaClusterResolver.class.getName(), kafkaOffsetCommitterConfiguration.getResolverClass());
    Assert.assertEquals("127.0.0.1:9093", kafkaOffsetCommitterConfiguration.getBootstrapServers());

    Properties kafkaConsumerProperties =
        kafkaOffsetCommitterConfiguration.getKafkaConsumerProperties(
            "cluster", "consumerGroup", false);
    Assert.assertEquals(
        "127.0.0.1:9093",
        kafkaConsumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    String clientID = kafkaConsumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
    Assert.assertTrue(clientID.startsWith("kafka-consumer-proxy-offset-committer"));
    Assert.assertEquals(
        "consumerGroup", kafkaConsumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        kafkaConsumerProperties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        kafkaConsumerProperties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    Assert.assertNull(
        kafkaConsumerProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assert.assertNull(
        kafkaConsumerProperties.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assert.assertNull(
        kafkaConsumerProperties.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertNull(
        kafkaConsumerProperties.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
  }

  @Test
  public void testWithSecurity() throws Exception {
    Assert.assertEquals(
        KafkaClusterResolver.class.getName(), kafkaOffsetCommitterConfiguration.getResolverClass());
    Assert.assertEquals("127.0.0.1:9093", kafkaOffsetCommitterConfiguration.getBootstrapServers());

    Properties kafkaConsumerProperties =
        kafkaOffsetCommitterConfiguration.getKafkaConsumerProperties(
            "cluster", "consumerGroup", true);
    Assert.assertEquals(
        "127.0.0.1:9093",
        kafkaConsumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    String clientID = kafkaConsumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
    Assert.assertTrue(clientID.startsWith("kafka-consumer-proxy-offset-committer"));
    Assert.assertEquals(
        "consumerGroup", kafkaConsumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        kafkaConsumerProperties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        kafkaConsumerProperties.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SECURITY_PROTOCOL,
        kafkaConsumerProperties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        kafkaConsumerProperties.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        kafkaConsumerProperties.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SECURITY_PROVIDER_CLASS,
        kafkaConsumerProperties.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
  }
}
