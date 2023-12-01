package com.uber.data.kafka.datatransfer.controller.config;

import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
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
@SpringBootTest(classes = {ZookeeperConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class ZookeeperConfigurationTest extends FievelTestBase {
  @Autowired ZookeeperConfiguration zookeeperConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(
        "localhost:2181/kafka-consumer-proxy", zookeeperConfiguration.getZkConnection());
  }
}
