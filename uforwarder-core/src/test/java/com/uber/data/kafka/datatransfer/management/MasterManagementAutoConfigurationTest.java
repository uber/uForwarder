package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.NodeAutoConfiguration;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.controller.storage.StoreAutoConfiguration;
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
@SpringBootTest(
  classes = {
    MasterManagementAutoConfiguration.class,
    StoreAutoConfiguration.class,
    NodeAutoConfiguration.class,
  }
)
@ActiveProfiles(profiles = {"data-transfer-controller"})
@TestPropertySource(
  properties = {"spring.config.location=classpath:/base.yaml", "master.store.enabled=false"}
)
public class MasterManagementAutoConfigurationTest extends FievelTestBase {
  @MockBean LeaderSelector leaderSelector;
  @MockBean ReadStore<Long, StoredJob> jobStore;
  @MockBean Store<String, StoredJobGroup> jobGroupStore;
  @MockBean Store<Long, StoredWorker> workerStore;
  @MockBean Node node;
  @MockBean NodeUrlResolver nodeUrlResolver;

  @Autowired MastersHtml mastersHtml;
  @Autowired MastersJson mastersJson;
  @Autowired WorkersHtml workersHtml;
  @Autowired WorkersJson workersJson;
  @Autowired JobsHtml jobsHtml;
  @Autowired MasterJobsJson jobsJson;
  @Autowired NavJson navJson;

  @Test
  public void test() {
    Assert.assertNotNull(mastersHtml);
    Assert.assertNotNull(mastersJson);
    Assert.assertNotNull(workersHtml);
    Assert.assertNotNull(workersJson);
    Assert.assertNotNull(jobsHtml);
    Assert.assertNotNull(jobsJson);
    Assert.assertNotNull(navJson);
  }
}
