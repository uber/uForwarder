package com.uber.data.kafka.datatransfer.management;

import static org.junit.Assert.assertEquals;

import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties(ManagementServerConfiguration.class)
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ManagementServerConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class ManagementServerConfigurationTest extends FievelTestBase {

  @Autowired private ManagementServerConfiguration managementServerConfiguration;

  @Test
  public void testConfiguration() {
    assertEquals("http://%s:8080", managementServerConfiguration.getDebugUrlFormat());
  }
}
