package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingAndMetricsStoreDecoratorTest extends FievelTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(LoggingAndMetricsStoreDecoratorTest.class);
  private Store<Long, StoredWorker> implStore;
  private Store<Long, StoredWorker> wrappedStore;

  @Before
  public void setup() {
    implStore = Mockito.mock(Store.class);
    wrappedStore =
        LoggingAndMetricsStoreDecorator.decorate(
            StoredWorker.newBuilder().build(), logger, CoreInfra.NOOP, implStore);
  }

  @Test
  public void start() {
    wrappedStore.start();
    Mockito.verify(implStore, Mockito.times(1)).start();
  }

  @Test
  public void stop() {
    wrappedStore.stop();
    Mockito.verify(implStore, Mockito.times(1)).stop();
  }

  @Test
  public void isRunning() {
    Mockito.when(implStore.isRunning()).thenReturn(true);
    Assert.assertTrue(wrappedStore.isRunning());
  }

  @Test
  public void initialized() throws Exception {
    CompletableFuture<Map<Long, Versioned<StoredWorker>>> future = new CompletableFuture<>();
    Mockito.when(implStore.initialized()).thenReturn(future);
    Assert.assertTrue(future == wrappedStore.initialized());
  }

  @Test
  public void getAllSuccess() throws Exception {
    Map<Long, Versioned<StoredWorker>> data =
        ImmutableMap.of(
            1L, VersionedProto.from(newWorker(1L)),
            2L, VersionedProto.from(newWorker(2L)));
    Mockito.when(implStore.getAll()).thenReturn(data);
    Assert.assertEquals(data, wrappedStore.getAll());
  }

  @Test(expected = RuntimeException.class)
  public void getAllFailure() throws Exception {
    Mockito.when(implStore.getAll()).thenThrow(new IllegalStateException());
    wrappedStore.getAll();
  }

  @Test
  public void getAllSelectorSuccess() throws Exception {
    Map<Long, Versioned<StoredWorker>> data =
        ImmutableMap.of(1L, VersionedProto.from(newWorker(1L)));
    Function<StoredWorker, Boolean> selector = w -> w.getNode().getId() == 1L;
    Mockito.when(implStore.getAll(ArgumentMatchers.eq(selector))).thenReturn(data);
    Assert.assertEquals(1, wrappedStore.getAll(selector).size());
  }

  @Test(expected = RuntimeException.class)
  public void getAllSelectorFailure() throws Exception {
    Function<StoredWorker, Boolean> selector = w -> w.getNode().getId() == 1L;
    Mockito.when(implStore.getAll(ArgumentMatchers.eq(selector)))
        .thenThrow(new IllegalStateException());
    wrappedStore.getAll(selector);
  }

  @Test
  public void createSuccess() throws Exception {
    BiFunction<Long, StoredWorker, StoredWorker> creatorFunction =
        (k, v) -> {
          return v;
        };
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.create(Mockito.any(), Mockito.any()))
        .thenReturn(VersionedProto.from(worker));
    Versioned<StoredWorker> createdWorker = wrappedStore.create(worker, creatorFunction);
    Assert.assertEquals(worker, createdWorker.model());
  }

  @Test(expected = RuntimeException.class)
  public void createFailure() throws Exception {
    BiFunction<Long, StoredWorker, StoredWorker> creatorFunction =
        (k, v) -> {
          return v;
        };
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.create(Mockito.any(), Mockito.any()))
        .thenThrow(new IllegalStateException());
    wrappedStore.create(worker, creatorFunction);
  }

  @Test
  public void getSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.get(ArgumentMatchers.eq(1L))).thenReturn(VersionedProto.from(worker));
    Assert.assertEquals(worker, wrappedStore.get(1L).model());
  }

  @Test(expected = RuntimeException.class)
  public void getFailure() throws Exception {
    Mockito.when(implStore.get(ArgumentMatchers.eq(1L))).thenThrow(new IllegalStateException());
    wrappedStore.get(1L);
  }

  @Test
  public void getThroughSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.getThrough(ArgumentMatchers.eq(1L)))
        .thenReturn(VersionedProto.from(worker));
    Assert.assertEquals(worker, wrappedStore.getThrough(1L).model());
  }

  @Test(expected = RuntimeException.class)
  public void getThroughFailure() throws Exception {
    Mockito.when(implStore.getThrough(ArgumentMatchers.eq(1L)))
        .thenThrow(new IllegalStateException());
    wrappedStore.getThrough(1L);
  }

  @Test
  public void putSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    wrappedStore.put(1L, versioned);
  }

  @Test(expected = RuntimeException.class)
  public void putFailure() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    Mockito.doThrow(new IllegalStateException()).when(implStore).put(Mockito.any(), Mockito.any());
    wrappedStore.put(1L, versioned);
  }

  @Test
  public void putAsyncSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    CompletableFuture future = CompletableFuture.completedFuture(null);
    Mockito.when(implStore.putAsync(Mockito.anyLong(), Mockito.any())).thenReturn(future);
    Assert.assertTrue(wrappedStore.putAsync(1L, versioned).toCompletableFuture().isDone());
  }

  @Test(expected = RuntimeException.class)
  public void putAsyncFailure() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    Mockito.doThrow(new IllegalStateException())
        .when(implStore)
        .putAsync(Mockito.any(), Mockito.any());
    wrappedStore.putAsync(1L, versioned).toCompletableFuture().get();
  }

  @Test
  public void testPutThrough() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    wrappedStore.putThrough(1L, versioned);
  }

  @Test
  public void removeSuccess() throws Exception {
    wrappedStore.remove(1L);
  }

  @Test(expected = RuntimeException.class)
  public void removeFailure() throws Exception {
    Mockito.doThrow(new IllegalStateException()).when(implStore).remove(Mockito.any());
    wrappedStore.remove(1L);
  }

  private static StoredWorker newWorker(long id) {
    StoredWorker.Builder builder = StoredWorker.newBuilder();
    builder.getNodeBuilder().setId(id);
    return builder.build();
  }
}
