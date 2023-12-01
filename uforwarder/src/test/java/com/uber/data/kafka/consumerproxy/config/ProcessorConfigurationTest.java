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
@SpringBootTest(classes = {ProcessorConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class ProcessorConfigurationTest extends FievelTestBase {
  @Autowired private ProcessorConfiguration processorConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(2, processorConfiguration.getThreadPoolSize());
    Assert.assertEquals(251, processorConfiguration.getMaxOutboundCacheCount());
    Assert.assertEquals(1001, processorConfiguration.getMaxInboundCacheCount());
    Assert.assertEquals(10001, processorConfiguration.getMaxAckCommitSkew());
    Assert.assertEquals(true, processorConfiguration.isClusterFilterEnabled());
    Assert.assertEquals(true, processorConfiguration.isExperimentalLimiterEnabled());
  }
}
