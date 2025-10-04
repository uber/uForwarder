package com.uber.data.kafka.datatransfer.controller.rpc;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
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
@SpringBootTest(classes = {RpcAutoConfiguration.class})
@ActiveProfiles(profiles = {"data-transfer-controller"})
@TestPropertySource(
    properties = {"spring.config.location=classpath:/base.yaml", "master.store.enabled=false"})
public class RpcAutoConfigurationTest extends FievelTestBase {
  @MockBean Scope scope;
  @MockBean CoreInfra coreInfra;
  @MockBean Node node;
  @MockBean Store<Long, StoredWorker> workerStore;
  @MockBean Store<Long, StoredJobStatus> jobStatusStore;
  @MockBean Store<String, StoredJobGroup> jobGroupStore;
  @MockBean ReadStore<Long, StoredJob> jobStore;
  @MockBean LeaderSelector leaderSelector;

  @Autowired ControllerWorkerService controllerWorkerService;
  @Autowired ControllerAdminService controllerAdminService;

  @Test
  public void test() {
    Assert.assertNotNull(controllerAdminService);
    Assert.assertNotNull(controllerWorkerService);
  }
}
