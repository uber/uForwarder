package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.MetricsConfiguration;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PipelineManagerAutoConfiguration.class, MetricsConfiguration.class})
@ActiveProfiles(profiles = {"data-transfer-worker"})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class PipelineManagerAutoConfigurationTest extends FievelTestBase {
  @MockBean PipelineFactory pipelineFactory;

  @MockBean Node Node;

  @Autowired PipelineManager pipelineManager;

  @Test
  public void test() {
    Assert.assertNotNull(pipelineManager);
  }
}
