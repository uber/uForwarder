package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.controller.config.JobGroupStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.JobStatusStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.WorkerStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.ZookeeperConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Scope;
import java.time.Duration;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreAutoConfigurationTest extends FievelTestBase {
  private CoreInfra infra;
  private LeaderSelector leaderSelector;

  private TestingServer zkServer;
  private ZookeeperConfiguration zkConfiguration;
  private int port;

  private StoreAutoConfiguration storeAutoConfiguration;

  private JsonFormat.TypeRegistry typeRegistry;

  @Before
  public void setup() throws Exception {
    Scope scope = Mockito.mock(Scope.class);
    infra = CoreInfra.NOOP;
    leaderSelector = Mockito.mock(LeaderSelector.class);

    // try at most 5 times
    for (int i = 0; i < 5; i++) {
      try {
        port = TestUtils.getRandomPort();
        zkServer = new TestingServer(port, true);
        break;
      } catch (IllegalStateException e) {
        if (i == 4) {
          throw e;
        }
      }
    }
    zkServer.start();

    zkConfiguration = new ZookeeperConfiguration();
    zkConfiguration.setZkConnection("localhost:" + port);
    storeAutoConfiguration = new StoreAutoConfiguration();
    typeRegistry = JsonFormat.TypeRegistry.newBuilder().build();

    Mockito.when(scope.subScope(Mockito.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(Mockito.anyMap())).thenReturn(scope);
  }

  @After
  public void teardown() throws Exception {
    zkServer.close();
  }

  @Test
  public void testJobStore() throws Exception {
    Assert.assertNotNull(
        storeAutoConfiguration.jobStore(
            storeAutoConfiguration.jobGroupStore(
                new JobGroupStoreConfiguration(),
                zkConfiguration,
                infra,
                typeRegistry,
                storeAutoConfiguration.jobGroupIdProvider(),
                leaderSelector)));
  }

  @Test
  public void testJobGroupStore() throws Exception {
    Assert.assertNotNull(
        storeAutoConfiguration.jobGroupStore(
            new JobGroupStoreConfiguration(),
            zkConfiguration,
            infra,
            typeRegistry,
            storeAutoConfiguration.jobGroupIdProvider(),
            leaderSelector));
  }

  @Test
  public void testJobStatusStore() throws Exception {
    Assert.assertNotNull(
        storeAutoConfiguration.jobStatusStore(
            new JobStatusStoreConfiguration(),
            zkConfiguration,
            infra,
            typeRegistry,
            storeAutoConfiguration.jobStatusIdProvider(),
            leaderSelector));
  }

  @Test
  public void testWorkerStore() throws Exception {
    WorkerStoreConfiguration config = new WorkerStoreConfiguration();
    config.setTtl(Duration.ofSeconds(1));
    Assert.assertNotNull(
        storeAutoConfiguration.workerStore(
            config,
            zkConfiguration,
            infra,
            typeRegistry,
            storeAutoConfiguration.workerIdProvider(config, zkConfiguration),
            leaderSelector));
  }

  @Test
  public void testJobIdProvider() throws Exception {
    Assert.assertNotNull(storeAutoConfiguration.jobGroupIdProvider());
  }
}
