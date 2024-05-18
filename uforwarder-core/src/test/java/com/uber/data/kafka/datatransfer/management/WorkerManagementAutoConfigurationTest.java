package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.MetricsConfiguration;
import com.uber.data.kafka.datatransfer.common.NodeAutoConfiguration;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineFactory;
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
@SpringBootTest(
  classes = {
    WorkerManagementAutoConfiguration.class,
    NodeAutoConfiguration.class,
    MetricsConfiguration.class
  }
)
@ActiveProfiles(profiles = {"data-transfer-worker"})
@TestPropertySource(
  properties = {"spring.config.location=classpath:/base.yaml", "master.store.enabled=false"}
)
public class WorkerManagementAutoConfigurationTest extends FievelTestBase {
  @MockBean Store<String, StoredJobGroup> jobGroupStore;
  @MockBean PipelineFactory pipelineFactory;
  @MockBean Node node;

  @Autowired WorkerJobsJson jobsJson;
  @Autowired JobsHtml jobsHtml;
  @Autowired JobStatusJson jobStatusJson;
  @Autowired JobStatusHtml jobStatusHtml;
  @Autowired NavJson navJson;

  @Test
  public void test() {
    Assert.assertNotNull(jobsHtml);
    Assert.assertNotNull(jobsJson);
    Assert.assertNotNull(jobStatusJson);
    Assert.assertNotNull(jobStatusHtml);
    Assert.assertNotNull(navJson);
  }
}
