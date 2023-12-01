package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class BufferedWriteDecoratorTest extends FievelTestBase {
  private Logger logger;
  private Store<Long, StoredWorker> implStore;
  private BufferedWriteDecorator<Long, StoredWorker> wrappedStore;

  @Before
  public void setUp() {
    logger = Mockito.mock(Logger.class);
    implStore = Mockito.mock(Store.class);
    wrappedStore =
        (BufferedWriteDecorator<Long, StoredWorker>)
            BufferedWriteDecorator.decorate(
                Duration.ofMillis(10), logger, CoreInfra.NOOP, implStore);
  }

  @Test
  public void testNoError() throws Exception {
    wrappedStore.isRunning();
    wrappedStore.initialized();
    wrappedStore.getAll();
    wrappedStore.getAll(v -> true);
    wrappedStore.create(StoredWorker.newBuilder().build(), (k, v) -> v);
    wrappedStore.get(0L);
    wrappedStore.getThrough(0L);
  }

  @Test
  public void testStartStop() {
    wrappedStore.start();
    wrappedStore.stop();
    wrappedStore.stop();
  }

  @Test
  public void testPutAsync() throws Exception {
    wrappedStore
        .putAsync(0L, Versioned.from(StoredWorker.newBuilder().build(), -1))
        .toCompletableFuture()
        .get();
  }

  @Test
  public void testPutThrough() throws Exception {
    Versioned<StoredWorker> worker = Versioned.from(StoredWorker.getDefaultInstance(), 1);
    wrappedStore.putThrough(1L, worker);
    Mockito.verify(implStore, Mockito.times(1)).putThrough(1L, worker);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testCache() throws Exception {
    wrappedStore.put(1L, Versioned.from(StoredWorker.newBuilder().build(), 2));
    wrappedStore.put(2L, Versioned.from(StoredWorker.newBuilder().build(), 2));
    wrappedStore.put(3L, Versioned.from(StoredWorker.newBuilder().build(), 2));
    Assert.assertEquals(3, ((BufferedWriteDecorator) wrappedStore).writeCache.size());
    wrappedStore.remove(3L);
    Assert.assertEquals(2, ((BufferedWriteDecorator) wrappedStore).writeCache.size());
    wrappedStore.start();
    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertEquals(0, ((BufferedWriteDecorator) wrappedStore).writeCache.size()));
  }

  @Test
  public void testRun() throws Exception {
    Versioned<StoredWorker> item = Versioned.from(StoredWorker.newBuilder().build(), 2);
    wrappedStore.put(1L, item);

    Mockito.when(implStore.putAsync(1L, item)).thenReturn(CompletableFuture.completedFuture(null));
    wrappedStore.run();
  }

  @Test
  public void testRunWithException() throws Exception {
    Versioned<StoredWorker> item = Versioned.from(StoredWorker.newBuilder().build(), 2);
    wrappedStore.put(1L, item);

    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException());
    Mockito.when(implStore.putAsync(1L, item)).thenReturn(future);
    wrappedStore.run();
  }
}
