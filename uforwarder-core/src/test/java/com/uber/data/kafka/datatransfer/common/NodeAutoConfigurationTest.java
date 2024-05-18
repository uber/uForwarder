package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {NodeAutoConfiguration.class})
@ActiveProfiles(profiles = {"uforwarder-worker"})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class NodeAutoConfigurationTest extends FievelTestBase {

  @Autowired Node node;

  @Test
  public void test() {
    Assert.assertNotNull(node);
    Assert.assertEquals(1000, node.getPort());
  }
}
