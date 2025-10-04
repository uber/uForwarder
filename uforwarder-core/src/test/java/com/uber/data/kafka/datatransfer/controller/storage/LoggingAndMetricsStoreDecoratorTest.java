package com.uber.data.kafka.datatransfer.controller.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingAndMetricsStoreDecoratorTest {
  private static final Logger logger =
      LoggerFactory.getLogger(LoggingAndMetricsStoreDecoratorTest.class);
  private Store<Long, StoredWorker> implStore;
  private Store<Long, StoredWorker> wrappedStore;

  @BeforeEach
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
    Assertions.assertTrue(wrappedStore.isRunning());
  }

  @Test
  public void initialized() throws Exception {
    CompletableFuture<Map<Long, Versioned<StoredWorker>>> future = new CompletableFuture<>();
    Mockito.when(implStore.initialized()).thenReturn(future);
    Assertions.assertTrue(future == wrappedStore.initialized());
  }

  @Test
  public void getAllSuccess() throws Exception {
    Map<Long, Versioned<StoredWorker>> data =
        ImmutableMap.of(
            1L, VersionedProto.from(newWorker(1L)),
            2L, VersionedProto.from(newWorker(2L)));
    Mockito.when(implStore.getAll()).thenReturn(data);
    Assertions.assertEquals(data, wrappedStore.getAll());
  }

  @Test
  public void getAllFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          Mockito.when(implStore.getAll()).thenThrow(new IllegalStateException());
          wrappedStore.getAll();
        });
  }

  @Test
  public void getAllSelectorSuccess() throws Exception {
    Map<Long, Versioned<StoredWorker>> data =
        ImmutableMap.of(1L, VersionedProto.from(newWorker(1L)));
    Function<StoredWorker, Boolean> selector = w -> w.getNode().getId() == 1L;
    Mockito.when(implStore.getAll(ArgumentMatchers.eq(selector))).thenReturn(data);
    Assertions.assertEquals(1, wrappedStore.getAll(selector).size());
  }

  @Test
  public void getAllSelectorFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          Function<StoredWorker, Boolean> selector = w -> w.getNode().getId() == 1L;
          Mockito.when(implStore.getAll(ArgumentMatchers.eq(selector)))
              .thenThrow(new IllegalStateException());
          wrappedStore.getAll(selector);
        });
  }

  @Test
  public void createSuccess() throws Exception {
    BiFunction<Long, StoredWorker, StoredWorker> creatorFunction = (k, v) -> v;
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.create(Mockito.any(), Mockito.any()))
        .thenReturn(VersionedProto.from(worker));
    Versioned<StoredWorker> createdWorker = wrappedStore.create(worker, creatorFunction);
    Assertions.assertEquals(worker, createdWorker.model());
  }

  @Test
  public void createFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          BiFunction<Long, StoredWorker, StoredWorker> creatorFunction = (k, v) -> v;
          StoredWorker worker = newWorker(1L);
          Mockito.when(implStore.create(Mockito.any(), Mockito.any()))
              .thenThrow(new IllegalStateException());
          wrappedStore.create(worker, creatorFunction);
        });
  }

  @Test
  public void getSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.get(ArgumentMatchers.eq(1L))).thenReturn(VersionedProto.from(worker));
    Assertions.assertEquals(worker, wrappedStore.get(1L).model());
  }

  @Test
  public void getFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          Mockito.when(implStore.get(ArgumentMatchers.eq(1L)))
              .thenThrow(new IllegalStateException());
          wrappedStore.get(1L);
        });
  }

  @Test
  public void getThroughSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Mockito.when(implStore.getThrough(ArgumentMatchers.eq(1L)))
        .thenReturn(VersionedProto.from(worker));
    Assertions.assertEquals(worker, wrappedStore.getThrough(1L).model());
  }

  @Test
  public void getThroughFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          Mockito.when(implStore.getThrough(ArgumentMatchers.eq(1L)))
              .thenThrow(new IllegalStateException());
          wrappedStore.getThrough(1L);
        });
  }

  @Test
  public void putSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    wrappedStore.put(1L, versioned);
  }

  @Test
  public void putFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          StoredWorker worker = newWorker(1L);
          Versioned<StoredWorker> versioned = VersionedProto.from(worker);
          Mockito.doThrow(new IllegalStateException())
              .when(implStore)
              .put(Mockito.any(), Mockito.any());
          wrappedStore.put(1L, versioned);
        });
  }

  @Test
  public void putAsyncSuccess() throws Exception {
    StoredWorker worker = newWorker(1L);
    Versioned<StoredWorker> versioned = VersionedProto.from(worker);
    CompletableFuture future = CompletableFuture.completedFuture(null);
    Mockito.when(implStore.putAsync(Mockito.anyLong(), Mockito.any())).thenReturn(future);
    Assertions.assertTrue(wrappedStore.putAsync(1L, versioned).toCompletableFuture().isDone());
  }

  @Test
  public void putAsyncFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          StoredWorker worker = newWorker(1L);
          Versioned<StoredWorker> versioned = VersionedProto.from(worker);
          Mockito.doThrow(new IllegalStateException())
              .when(implStore)
              .putAsync(Mockito.any(), Mockito.any());
          wrappedStore.putAsync(1L, versioned).toCompletableFuture().get();
        });
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

  @Test
  public void removeFailure() throws Exception {
    assertThrows(
        RuntimeException.class,
        () -> {
          Mockito.doThrow(new IllegalStateException()).when(implStore).remove(Mockito.any());
          wrappedStore.remove(1L);
        });
  }

  private static StoredWorker newWorker(long id) {
    StoredWorker.Builder builder = StoredWorker.newBuilder();
    builder.getNodeBuilder().setId(id);
    return builder.build();
  }
}
