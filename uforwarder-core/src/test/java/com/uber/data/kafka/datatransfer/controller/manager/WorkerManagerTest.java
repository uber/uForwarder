package com.uber.data.kafka.datatransfer.controller.manager;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class WorkerManagerTest extends FievelTestBase {
  private Store<Long, StoredWorker> workerStore;
  private WorkerManager workerManager;

  @Before
  public void setup() throws Exception {
    LeaderSelector leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);
    workerStore = Mockito.mock(Store.class);

    CompletableFuture initializedFuture = new CompletableFuture();
    initializedFuture.complete(ImmutableMap.of());
    Mockito.when(workerStore.initialized()).thenReturn(initializedFuture);
    Map<Long, Versioned<StoredWorker>> workers =
        ImmutableMap.of(1L, VersionedProto.from(newWorker(1L)));
    Mockito.when(workerStore.getAll()).thenReturn(workers);

    workerManager = new WorkerManager(new NoopScope(), workerStore, leaderSelector);
  }

  @Test
  public void logAndMetrics() {
    workerManager.logAndMetrics();
  }

  @Test
  public void logAndMetricsWithException() throws Exception {
    Mockito.doThrow(new RuntimeException()).when(workerStore).getAll();
    workerManager.logAndMetrics();
  }

  private static StoredWorker newWorker(long id) {
    StoredWorker.Builder builder = StoredWorker.newBuilder();
    builder.getNodeBuilder().setId(id);
    return builder.build();
  }
}
