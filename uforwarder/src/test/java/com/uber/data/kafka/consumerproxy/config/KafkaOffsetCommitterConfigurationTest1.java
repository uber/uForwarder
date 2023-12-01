package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
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
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class KafkaOffsetCommitterConfigurationTest1 extends FievelTestBase {
  @Autowired private KafkaOffsetCommitterConfiguration kafkaOffsetCommitterConfiguration;

  @Test
  public void test() throws Exception {
    Assert.assertEquals(
        "com.uber.data.kafka.datatransfer.common.StreamingCommonKafkaClusterResolver",
        kafkaOffsetCommitterConfiguration.getResolverClass());
    Assert.assertEquals("127.0.0.1:9093", kafkaOffsetCommitterConfiguration.getBootstrapServers());

    Exception exception = null;
    try {
      kafkaOffsetCommitterConfiguration.getKafkaConsumerProperties(
          "cluster", "consumerGroup", false);
    } catch (Exception e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }
}
