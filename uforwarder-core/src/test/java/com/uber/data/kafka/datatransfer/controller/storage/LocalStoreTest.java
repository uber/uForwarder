package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.NoSuchElementException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class LocalStoreTest extends FievelTestBase {
  private IdProvider<Long, StoredWorker> idProvider;
  private LocalStore<Long, StoredWorker> store;

  @Before
  public void setup() throws Exception {
    idProvider = Mockito.mock(IdProvider.class);
    store = new LocalStore<>(idProvider);
    store.put(1L, VersionedProto.from(StoredWorker.newBuilder().build()));
  }

  @Test
  public void initialized() throws Exception {
    Assert.assertEquals(1, store.initialized().get().size());
  }

  @Test
  public void getAll() throws Exception {
    Assert.assertEquals(1, store.getAll().size());
    Assert.assertEquals(1, store.getAll(w -> true).size());
  }

  @Test
  public void get() throws Exception {
    Assert.assertNotNull(store.get(1L));
  }

  @Test(expected = NoSuchElementException.class)
  public void getFailure() throws Exception {
    Assert.assertNotNull(store.get(2L));
  }

  @Test
  public void getThrough() throws Exception {
    Assert.assertNotNull(store.getThrough(1L));
  }

  @Test
  public void create() throws Exception {
    Mockito.when(idProvider.getId(Mockito.any())).thenReturn(2L);
    Assert.assertNotNull(store.create(StoredWorker.newBuilder().build(), (k, v) -> v));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createFailure() throws Exception {
    Mockito.when(idProvider.getId(Mockito.any())).thenReturn(1L);
    store.create(StoredWorker.newBuilder().build(), (k, v) -> v);
  }

  @Test
  public void put() throws Exception {
    store.put(2L, VersionedProto.from(StoredWorker.newBuilder().build()));
    Assert.assertEquals(2, store.getAll().size());
  }

  @Test
  public void putAsync() throws Exception {
    store
        .putAsync(2L, VersionedProto.from(StoredWorker.newBuilder().build()))
        .toCompletableFuture()
        .get();
    Assert.assertEquals(2, store.getAll().size());
  }

  @Test
  public void putThrough() throws Exception {
    store.putThrough(2L, VersionedProto.from(StoredWorker.newBuilder().build()));
    Assert.assertEquals(2, store.getAll().size());
  }

  @Test
  public void remove() throws Exception {
    store.remove(1L);
    Assert.assertEquals(0, store.getAll().size());
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
    Assert.assertTrue(store.isRunning());
  }
}
