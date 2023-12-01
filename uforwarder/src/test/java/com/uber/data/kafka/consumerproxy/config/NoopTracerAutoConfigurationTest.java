package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import io.opentracing.Tracer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {NoopTracerAutoConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class NoopTracerAutoConfigurationTest extends FievelTestBase {
  @Autowired private Tracer tracer;

  @Test
  public void testTracer() {
    Assert.assertNotNull(this.tracer);
  }
}
