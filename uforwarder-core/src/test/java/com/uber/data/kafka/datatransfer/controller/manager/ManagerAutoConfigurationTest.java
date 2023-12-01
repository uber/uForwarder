package com.uber.data.kafka.datatransfer.controller.manager;

import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Scope;
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
@SpringBootTest(classes = {ManagerAutoConfiguration.class, WorkerStoreAutoConfiguration.class})
@ActiveProfiles(profiles = {"data-transfer-controller"})
@TestPropertySource(
  properties = {"spring.config.location=classpath:/base.yaml", "master.store.enabled=false"}
)
public class ManagerAutoConfigurationTest extends FievelTestBase {
  @MockBean Scope scope;
  @MockBean Rebalancer rebalancer;

  @MockBean ShadowRebalancerDelegate shadowRebalancerDelegate;

  @MockBean Store<String, StoredJobGroup> jobGroupStore;
  @MockBean Store<Long, StoredJobStatus> jobStatusStore;
  @MockBean LeaderSelector leaderSelector;

  @Autowired ManagerAutoConfiguration managerAutoConfiguration;
  @Autowired JobManager jobManager;
  @Autowired WorkerManager workerManager;

  @Test
  public void testAutowired() {
    Assert.assertNotNull(managerAutoConfiguration);
    Assert.assertNotNull(jobManager);
    Assert.assertNotNull(workerManager);
  }
}
