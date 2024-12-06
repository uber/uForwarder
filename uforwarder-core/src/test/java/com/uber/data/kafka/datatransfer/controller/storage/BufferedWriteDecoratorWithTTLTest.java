package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This test verifies that buffered write with ttl still updates ttl correctly.
public class BufferedWriteDecoratorWithTTLTest extends FievelTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(BufferedWriteDecoratorWithTTLTest.class);
  private static Duration WRITE_INTERVAL = Duration.ofSeconds(2);
  private static Duration WRITE_TTL = Duration.ofSeconds(10);
  private static Duration CHECK_INTERVAL_THROUGH = Duration.ofSeconds(5);
  private static Duration CHECK_INTERVAL_REMOVE = Duration.ofSeconds(12);
  private CoreInfra infra;
  private LeaderSelector leaderSelector;
  private Store<Long, StoredWorker> underlyingStore;
  private Store<Long, StoredWorker> workerStore;
  private ArgumentCaptor<String> stringCaptor;
  private ArgumentCaptor<Runnable> runnableCaptor;
  private StoredWorker worker;

  @Before
  public void setup() {
    worker = StoredWorker.newBuilder().setNode(Node.newBuilder().setId(1).build()).build();
    stringCaptor = ArgumentCaptor.forClass(String.class);
    runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.doReturn(true).when(leaderSelector).isLeader();
    infra = CoreInfra.NOOP;
    underlyingStore = Mockito.mock(Store.class);
    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(underlyingStore)
        .putAsync(Mockito.anyLong(), Mockito.any());
    workerStore = BufferedWriteDecorator.decorate(WRITE_INTERVAL, logger, infra, underlyingStore);
    workerStore =
        TTLDecorator.decorate(
            infra, workerStore, WorkerUtils::getWorkerId, WRITE_TTL, leaderSelector);
    workerStore.start();
  }

  @After
  public void teardown() {
    workerStore.stop();
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 20000)
  public void testPutUpdatesTTL() throws Exception {
    Versioned<StoredWorker> versionedWorker = Versioned.from(worker, 1);
    workerStore.put(1L, versionedWorker);
    // < 10s so we shouldn't have written through
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).putThrough(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).putAsync(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).remove(1L);

    // after 5s, we should write through
    Thread.sleep(CHECK_INTERVAL_THROUGH.toMillis());
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).putThrough(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(1)).putAsync(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).remove(1L);

    // after 10s, TTL should delete it
    Thread.sleep(CHECK_INTERVAL_REMOVE.toMillis());
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).putThrough(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(1)).putAsync(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(1)).remove(1L);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 13000)
  public void testPutThroughUpdatesTTL() throws Exception {
    Versioned<StoredWorker> versionedWorker = Versioned.from(worker, 1);
    workerStore.putThrough(1L, versionedWorker);
    // < 10s but we invoke write through so we should write through
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(1)).putThrough(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).putAsync(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).remove(1L);

    // after 10s, TTL should delete it
    Thread.sleep(CHECK_INTERVAL_REMOVE.toMillis());
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(1)).putThrough(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(0)).putAsync(1L, versionedWorker);
    Mockito.verify(underlyingStore, Mockito.times(1)).remove(1L);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 20000)
  public void testPutThenPutThroughPrefersPutThrough() throws Exception {
    Versioned<StoredWorker> workerV1 = Versioned.from(worker, 1);
    workerStore.put(1L, workerV1);
    // < 10s so we shouldn't have written through
    runAsLeader();

    // Put through < 10s
    Versioned<StoredWorker> workerV2 = Versioned.from(worker, 2);
    workerStore.putThrough(1L, workerV2);
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(0)).putThrough(1L, workerV1);
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, workerV1);
    Mockito.verify(underlyingStore, Mockito.times(0)).putAsync(1L, workerV1);
    Mockito.verify(underlyingStore, Mockito.times(1)).putThrough(1L, workerV2);
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, workerV2);
    Mockito.verify(underlyingStore, Mockito.times(0)).putAsync(1L, workerV2);
    Mockito.verify(underlyingStore, Mockito.times(0)).remove(1L);

    // since workerV2 is accepted with version = 2, zk will fail write for version = 1
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException());
    Mockito.doThrow(new RuntimeException()).when(underlyingStore).put(1L, workerV1);
    Mockito.doReturn(future).when(underlyingStore).putAsync(1L, workerV1);
    Mockito.doThrow(new RuntimeException()).when(underlyingStore).putThrough(1L, workerV1);

    // after 10s, TTL should delete it
    Thread.sleep(CHECK_INTERVAL_REMOVE.toMillis());
    runAsLeader();
    Mockito.verify(underlyingStore, Mockito.times(0)).putThrough(1L, workerV1);
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, workerV1);
    Mockito.verify(underlyingStore, Mockito.times(1)).putAsync(1L, workerV1);
    Mockito.verify(underlyingStore, Mockito.times(1)).putThrough(1L, workerV2);
    Mockito.verify(underlyingStore, Mockito.times(0)).put(1L, workerV2);
    Mockito.verify(underlyingStore, Mockito.times(0)).putAsync(1L, workerV2);
    Mockito.verify(underlyingStore, Mockito.times(1)).remove(1L);
  }

  private void runAsLeader() {
    Mockito.verify(leaderSelector, Mockito.atMost(100))
        .runIfLeader(stringCaptor.capture(), runnableCaptor.capture());
    runnableCaptor.getAllValues().forEach(r -> r.run());
  }
}
