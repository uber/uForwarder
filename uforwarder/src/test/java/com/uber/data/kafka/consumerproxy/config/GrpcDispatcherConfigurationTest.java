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
@SpringBootTest(classes = {GrpcDispatcherConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class GrpcDispatcherConfigurationTest extends FievelTestBase {
  @Autowired private GrpcDispatcherConfiguration grpcDispatcherConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(2, grpcDispatcherConfiguration.getMinRpcTimeoutMs());
    Assert.assertEquals(1800001, grpcDispatcherConfiguration.getMaxRpcTimeoutMs());
    Assert.assertEquals(2, grpcDispatcherConfiguration.getGrpcChannelPoolSize());
  }
}
