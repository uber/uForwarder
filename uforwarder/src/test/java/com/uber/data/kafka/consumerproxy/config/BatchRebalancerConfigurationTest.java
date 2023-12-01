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
@SpringBootTest(classes = {RebalancerConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/batch-job.yaml"})
public class BatchRebalancerConfigurationTest extends FievelTestBase {
  @Autowired private RebalancerConfiguration config;

  @Test
  public void test() {
    Assert.assertEquals("BatchRpcUriRebalancer", config.getMode());
    Assert.assertEquals(1, config.getNumWorkersPerUri());
    Assert.assertEquals(1000, config.getMessagesPerSecPerWorker());
    Assert.assertFalse(config.getShouldRunShadowRebalancer());
  }
}
