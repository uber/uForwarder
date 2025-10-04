package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
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
@SpringBootTest(
    classes = {
      KafkaDispatcherFactoryAutoConfiguration.class,
    })
@TestPropertySource(
    properties = {
      "spring.config.location=classpath:/base.yaml",
      "worker.dispatcher.kafka.enabled=true"
    })
public class KafkaDispatcherFactoryAutoConfigurationTest extends FievelTestBase {
  @Autowired KafkaDispatcherConfiguration config;
  @Autowired KafkaDispatcherFactory factory;

  @Test
  public void test() {
    Assert.assertNotNull(config);
    Assert.assertEquals(
        "com.uber.data.kafka.datatransfer.common.KafkaClusterResolver", config.getResolverClass());
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        config.getKeySerializerClass());
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        config.getValueSerializerClass());
    Assert.assertEquals("zstd", config.getCompressionType());
    Assert.assertNotNull(factory);
  }

  @Test
  public void testKafkaProperties() throws Exception {
    // create new config b/c changing the class level config will break the other test
    KafkaDispatcherConfiguration config = new KafkaDispatcherConfiguration();

    Properties propsWithoutSecurity = config.getProperties("my-cluster", "client-id", false, false);
    Assert.assertEquals(
        "client-id", propsWithoutSecurity.getProperty(ProducerConfig.CLIENT_ID_CONFIG));
    Assert.assertEquals(
        "localhost:9092",
        propsWithoutSecurity.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals("all", propsWithoutSecurity.getProperty(ProducerConfig.ACKS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        propsWithoutSecurity.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        propsWithoutSecurity.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    Assert.assertNull(
        propsWithoutSecurity.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assert.assertNull(propsWithoutSecurity.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assert.assertNull(
        propsWithoutSecurity.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertNull(propsWithoutSecurity.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
    Assert.assertEquals(
        "snappy", propsWithoutSecurity.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));

    Properties propsWithSecurity = config.getProperties("my-cluster", "client-id", true, false);
    Assert.assertEquals(
        "client-id", propsWithSecurity.getProperty(ProducerConfig.CLIENT_ID_CONFIG));
    Assert.assertEquals(
        "localhost:9092", propsWithSecurity.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals("all", propsWithSecurity.getProperty(ProducerConfig.ACKS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        propsWithSecurity.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        "org.apache.kafka.common.serialization.ByteArraySerializer",
        propsWithSecurity.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SECURITY_PROTOCOL,
        propsWithSecurity.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        propsWithSecurity.getProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SSL_ALGORITHM,
        propsWithSecurity.getProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG));
    Assert.assertEquals(
        KafkaUtils.SECURITY_PROVIDER_CLASS,
        propsWithSecurity.getProperty(SecurityConfig.SECURITY_PROVIDERS_CONFIG));
    Assert.assertEquals(
        "snappy", propsWithSecurity.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
  }
}
