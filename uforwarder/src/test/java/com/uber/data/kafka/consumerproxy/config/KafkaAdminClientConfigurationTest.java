package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
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
@SpringBootTest(classes = {KafkaAdminClientConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class KafkaAdminClientConfigurationTest extends FievelTestBase {
  @Autowired private KafkaAdminClientConfiguration kafkaAdminClientConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(
        "kafka-consumer-proxy-admin-test0", kafkaAdminClientConfiguration.getClientId());
    // make sure that each time we get a different client ID
    Assert.assertEquals(
        "kafka-consumer-proxy-admin-test1", kafkaAdminClientConfiguration.getClientId());
    Assert.assertEquals(
        "com.uber.data.kafka.datatransfer.common.StreamingCommonKafkaClusterResolver",
        kafkaAdminClientConfiguration.getResolverClass());
    Assert.assertEquals("127.0.0.1:9093", kafkaAdminClientConfiguration.getBootstrapServers());

    Exception exception = null;
    try {
      kafkaAdminClientConfiguration.getBootstrapServer("test");
    } catch (Exception e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    kafkaAdminClientConfiguration.setResolverClass(KafkaClusterResolver.class.getName());
    Properties properties = kafkaAdminClientConfiguration.getProperties("test");
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
