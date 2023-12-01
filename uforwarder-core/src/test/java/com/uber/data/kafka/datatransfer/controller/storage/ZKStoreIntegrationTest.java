package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.JsonSerializationFactory;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKStoreIntegrationTest extends FievelTestBase {
  private static final Logger logger = LoggerFactory.getLogger(ZKStoreIntegrationTest.class);
  private TestingServer zkServer;
  private CuratorFramework zkClient;
  private ZKStore<Long, StoredWorker> zkStore;

  @Before
  public void setup() throws Exception {
    int port = 0;
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
    zkClient = CuratorFrameworkFactory.newClient("localhost:" + port, new RetryOneTime(100));
    zkClient.start();
    try {
      zkClient.delete().deletingChildrenIfNeeded().forPath("/workers");
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        logger.warn("skipping delete for non-existent node");
      }
    }
    zkStore =
        new ZKStore<>(
            logger,
            CoreInfra.NOOP,
            AsyncCuratorFramework.wrap(zkClient),
            new JsonSerializationFactory<>(StoredWorker.newBuilder().build()),
            new ZKSequencer(zkClient, "/sequencer/workers"),
            WorkerUtils::getWorkerId,
            "/workers/{id}");
    zkStore.start();
  }

  @After
  public void teardown() throws Exception {
    zkStore.stop();
    zkClient.close();
    zkServer.close();
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void putGetAndRemove() throws Exception {
    // Verify that it is empty
    List<StoredWorker> list =
        zkStore.getAll().values().stream().map(item -> item.model()).collect(Collectors.toList());
    Assert.assertEquals(0, list.size());

    // Create a new worker
    final StoredWorker worker1 =
        StoredWorker.newBuilder().setNode(Node.newBuilder().setId(1).build()).build();
    Versioned<StoredWorker> insertedStoredWorker1 =
        zkStore.create(worker1, WorkerUtils::withWorkerId);
    // Should insert with id = 1 since this node does not yet exist.
    Assert.assertEquals(1, insertedStoredWorker1.model().getNode().getId());

    // Get or Create for existing worker should return worker
    // We use sleep to synchronize time for integration test b/c curator does not allow us to inject
    // a ticker for test purposes.
    Thread.sleep(2000); // sleep to give cache †ime to update.
    Assert.assertEquals(worker1, zkStore.get(WorkerUtils.getWorkerId(worker1)).model());

    // Put worker
    StoredWorker updatedStoredWorker1 =
        StoredWorker.newBuilder(worker1).setState(WorkerState.WORKER_STATE_REGISTERING).build();
    zkStore.put(1L, Versioned.from(updatedStoredWorker1, -1));
    Thread.sleep(2000); // sleep to give cache †ime to update
    Versioned<StoredWorker> versionedStoredWorker = zkStore.get(1L);
    Assert.assertNotNull(versionedStoredWorker);
    Assert.assertEquals(updatedStoredWorker1, versionedStoredWorker.model());

    Map<Long, Versioned<StoredWorker>> workerList = zkStore.getAll();
    Assert.assertEquals(1, workerList.size());

    // Remove worker
    zkStore.remove(1L);
    Thread.sleep(2000); // sleep to give cache †ime to update
    Assert.assertEquals(0, zkStore.getAll().size());
  }
}
