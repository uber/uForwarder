package com.uber.data.kafka.datatransfer.controller.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LocalStoreTest {
  private IdProvider<Long, StoredWorker> idProvider;
  private LocalStore<Long, StoredWorker> store;

  @BeforeEach
  public void setup() throws Exception {
    idProvider = Mockito.mock(IdProvider.class);
    store = new LocalStore<>(idProvider);
    store.put(1L, VersionedProto.from(StoredWorker.newBuilder().build()));
  }

  @Test
  public void initialized() throws Exception {
    Assertions.assertEquals(1, store.initialized().get().size());
  }

  @Test
  public void getAll() throws Exception {
    Assertions.assertEquals(1, store.getAll().size());
    Assertions.assertEquals(1, store.getAll(w -> true).size());
  }

  @Test
  public void get() throws Exception {
    Assertions.assertNotNull(store.get(1L));
  }

  @Test
  public void getFailure() throws Exception {
    assertThrows(NoSuchElementException.class, () -> Assertions.assertNotNull(store.get(2L)));
  }

  @Test
  public void getThrough() throws Exception {
    Assertions.assertNotNull(store.getThrough(1L));
  }

  @Test
  public void create() throws Exception {
    Mockito.when(idProvider.getId(Mockito.any())).thenReturn(2L);
    Assertions.assertNotNull(store.create(StoredWorker.newBuilder().build(), (k, v) -> v));
  }

  @Test
  public void createFailure() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Mockito.when(idProvider.getId(Mockito.any())).thenReturn(1L);
          store.create(StoredWorker.newBuilder().build(), (k, v) -> v);
        });
  }

  @Test
  public void put() throws Exception {
    store.put(2L, VersionedProto.from(StoredWorker.newBuilder().build()));
    Assertions.assertEquals(2, store.getAll().size());
  }

  @Test
  public void putAsync() throws Exception {
    store
        .putAsync(2L, VersionedProto.from(StoredWorker.newBuilder().build()))
        .toCompletableFuture()
        .get();
    Assertions.assertEquals(2, store.getAll().size());
  }

  @Test
  public void putThrough() throws Exception {
    store.putThrough(2L, VersionedProto.from(StoredWorker.newBuilder().build()));
    Assertions.assertEquals(2, store.getAll().size());
  }

  @Test
  public void remove() throws Exception {
    store.remove(1L);
    Assertions.assertEquals(0, store.getAll().size());
  }

  @Test
  public void start() {
    store.start();
  }

  @Test
  public void stop() {
    store.stop();
  }

  @Test
  public void isRunning() {
    Assertions.assertTrue(store.isRunning());
  }
}
