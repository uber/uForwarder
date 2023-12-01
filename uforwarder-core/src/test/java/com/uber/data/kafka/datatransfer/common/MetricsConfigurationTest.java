package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Scope;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {MetricsConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class MetricsConfigurationTest extends FievelTestBase {

  @Autowired private Scope scope;

  @Test
  public void testScope() {
    Assert.assertNotNull(scope);
  }
}
