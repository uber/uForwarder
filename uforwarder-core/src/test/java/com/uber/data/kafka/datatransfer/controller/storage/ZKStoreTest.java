package com.uber.data.kafka.datatransfer.controller.storage;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.curator.x.async.modeled.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCache;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKStoreTest extends FievelTestBase {

  private static Logger logger = LoggerFactory.getLogger(ZKStoreTest.class);
  private ZKStore<Long, StoredWorker> zkStore;
  private CachedModeledFramework<StoredWorker> mockCachedModeledFramework;
  private ModeledCache<StoredWorker> mockModeledCache;
  private StoredWorker item1;
  private StoredWorker item2;

  @Before
  public void setup() {
    mockModeledCache = mock(ModeledCache.class);
    mockCachedModeledFramework = mock(CachedModeledFramework.class);
    when(mockCachedModeledFramework.cache()).thenReturn(mockModeledCache);
    zkStore =
        new ZKStore<Long, StoredWorker>(
            logger,
            CoreInfra.NOOP,
            mockCachedModeledFramework,
            new LocalSequencer(),
            WorkerUtils::getWorkerId,
            ZPath.parseWithIds("/workers/{id}"));
    item1 = StoredWorker.newBuilder().setNode(Node.newBuilder().setId(1).build()).build();
    item2 = StoredWorker.newBuilder().setNode(Node.newBuilder().setId(2).build()).build();
  }

  @Test
  public void lifecycle() throws Exception {
    List<ModeledCacheListener<StoredWorker>> listeners = new ArrayList<>();
    Listenable<ModeledCacheListener<StoredWorker>> mockListenable =
        new Listenable<ModeledCacheListener<StoredWorker>>() {
          @Override
          public void addListener(ModeledCacheListener<StoredWorker> listener) {
            listeners.add(listener);
          }

          @Override
          public void addListener(ModeledCacheListener<StoredWorker> listener, Executor executor) {
            listeners.add(listener);
          }

          @Override
          public void removeListener(ModeledCacheListener<StoredWorker> listener) {
            listeners.remove(listener);
          }
        };
    when(mockCachedModeledFramework.listenable()).thenReturn(mockListenable);
    AsyncCuratorFramework mockAsyncCuratorFramework = mock(AsyncCuratorFramework.class);
    CuratorFramework mockCuratorFramework = mock(CuratorFramework.class);
    when(mockCachedModeledFramework.unwrap()).thenReturn(mockAsyncCuratorFramework);
    when(mockAsyncCuratorFramework.unwrap()).thenReturn(mockCuratorFramework);
    when(mockCuratorFramework.getState()).thenReturn(CuratorFrameworkState.LATENT);

    CompletableFuture<Map<Long, Versioned<StoredWorker>>> future = zkStore.initialized();
    zkStore.start();
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(1, listeners.size());

    when(mockCachedModeledFramework.cache()).thenReturn(mockModeledCache);
    when(mockModeledCache.currentChildren(ZPath.root))
        .thenReturn(
            ImmutableMap.of(
                ZPath.parse("/workers/1"), new MockZNode<>(item1, ZPath.parse("/workers/1"), 1),
                ZPath.parse("/workers/2"), new MockZNode<>(item2, ZPath.parse("/workers/2"), 2)));
    for (ModeledCacheListener<StoredWorker> listener : listeners) {
      listener.initialized();
    }
    Assert.assertTrue(future.isDone());
    Map<Long, Versioned<StoredWorker>> jobs = future.get();
    Assert.assertEquals(2, jobs.size());
    Assert.assertEquals(item1, jobs.get(1L).model());
    Assert.assertEquals(item2, jobs.get(2L).model());

    Assert.assertTrue(zkStore.isRunning());
    // validate that zkstore gracefully handles zk client close exception.
    doThrow(new RuntimeException()).when(mockCachedModeledFramework).close();
    zkStore.stop();
  }

  @Test
  public void getAllWithException() throws Exception {
    Assert.assertEquals(0, zkStore.getAll().size());
    verify(mockModeledCache).currentChildren(ZPath.root);
  }

  @SuppressWarnings("LockNotBeforeTry")
  @Test(timeout = 10000)
  public void getAllConcurrently() throws Exception {
    Lock testSynchronizationLock = new ReentrantLock();
    testSynchronizationLock.lock();
    StoredWorker item1 =
        StoredWorker.newBuilder().setNode(Node.newBuilder().setId(1).build()).build();
    StoredWorker item2 =
        StoredWorker.newBuilder().setNode(Node.newBuilder().setId(2).build()).build();
    when(mockModeledCache.currentChildren(ZPath.root))
        .thenAnswer(
            new Answer<Map<ZPath, ZNode<StoredWorker>>>() {
              @Override
              public Map<ZPath, ZNode<StoredWorker>> answer(InvocationOnMock invocation)
                  throws Throwable {
                // This will block until the test unlocks it.
                testSynchronizationLock.lock();
                testSynchronizationLock.unlock();
                return ImmutableMap.of(
                    ZPath.parse("/workers/1"), new MockZNode<>(item1, ZPath.parse("/workers/1"), 1),
                    ZPath.parse("/workers/2"),
                        new MockZNode<>(item2, ZPath.parse("/workers/2"), 2));
              }
            });
    // future1 should contain the result after calling mockModeledCache.
    CompletableFuture<Map<Long, Versioned<StoredWorker>>> future1 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return zkStore.getAll();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    // future2 should have empty result since tryLock fails since execution1 is holding lock.
    CompletableFuture<Map<Long, Versioned<StoredWorker>>> future2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return zkStore.getAll();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    // unblock the mockModeledCache function.
    testSynchronizationLock.unlock();

    // Both futures should be completed
    CompletableFuture.allOf(future1, future2);

    // Both the future should have 2 items.
    Assert.assertTrue(future1.get().size() == 2 || future2.get().size() == 2);
  }

  @Test
  public void getAll() throws Exception {
    StoredWorker item1 =
        StoredWorker.newBuilder().setNode(Node.newBuilder().setId(1).build()).build();
    StoredWorker item2 =
        StoredWorker.newBuilder().setNode(Node.newBuilder().setId(2).build()).build();
    when(mockModeledCache.currentChildren(ZPath.root))
        .thenReturn(
            ImmutableMap.of(
                ZPath.parse("/workers/1"), new MockZNode<>(item1, ZPath.parse("/workers/1"), 1),
                ZPath.parse("/workers/2"), new MockZNode<>(item2, ZPath.parse("/workers/2"), 2)));

    Map<Long, Versioned<StoredWorker>> list = zkStore.getAll();

    Assert.assertEquals(2, list.size());
    Assert.assertEquals(VersionedProto.from(item1, 1), list.get(1L));
    Assert.assertEquals(VersionedProto.from(item2, 2), list.get(2L));
  }

  @Test
  public void getAllWithSelector() throws Exception {
    when(mockModeledCache.currentChildren(ZPath.root))
        .thenReturn(
            ImmutableMap.of(
                ZPath.parse("/workers/1"), new MockZNode<>(item1, ZPath.parse("/workers/1"), 1),
                ZPath.parse("/workers/2"), new MockZNode<>(item2, ZPath.parse("/workers/2"), 2)));

    Map<Long, Versioned<StoredWorker>> list = zkStore.getAll(j -> j.getNode().getId() == 1);

    Assert.assertEquals(1, list.size());
    Assert.assertEquals(1, list.get(1L).version());
    Assert.assertEquals(item1, list.get(1L).model());
  }

  @Test
  public void get() throws Exception {
    when(mockModeledCache.currentData(ZPath.parse("/workers/1")))
        .thenReturn(Optional.of(new MockZNode(item1, ZPath.parse("/workers/1"), 1)));
    Assert.assertEquals(VersionedProto.from(item1, 1), zkStore.get(1L));
  }

  @Test(expected = NoSuchElementException.class)
  public void getWithException() throws Exception {
    zkStore.get(1L);
  }

  @Test
  public void getThrough() throws Exception {
    CompletableFuture<ZNode<StoredWorker>> dataCompletableFuture = new CompletableFuture<>();
    dataCompletableFuture.complete(new MockZNode(item1, ZPath.parse("/workers/1"), 1));
    AsyncStage<ZNode<StoredWorker>> dataAsyncStage = mock(AsyncStage.class);
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    when(mockCachedModeledFramework.readThroughAsZNode()).thenReturn(dataAsyncStage);
    when(dataAsyncStage.toCompletableFuture()).thenReturn(dataCompletableFuture);
    Assert.assertEquals(VersionedProto.from(item1, 1), zkStore.getThrough(1L));
  }

  @Test
  public void create() throws Exception {
    // Mock set call
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    AsyncStage<String> putAsyncStage = mock(AsyncStage.class);
    when(mockCachedModeledFramework.set(item1, -1)).thenReturn(putAsyncStage);
    CompletableFuture<String> putCompletableFuture = new CompletableFuture<>();
    putCompletableFuture.complete("");
    when(putAsyncStage.toCompletableFuture()).thenReturn(putCompletableFuture);

    // Mock get through
    MockAsyncStage<ZNode<StoredWorker>> getCompletableFuture = new MockAsyncStage<>();
    when(mockCachedModeledFramework.readThroughAsZNode()).thenReturn(getCompletableFuture);
    getCompletableFuture.complete(
        new ZNode<StoredWorker>() {
          @Override
          public ZPath path() {
            return null;
          }

          @Override
          public Stat stat() {
            return new Stat();
          }

          @Override
          public StoredWorker model() {
            return item1;
          }
        });

    Assert.assertEquals(
        VersionedProto.from(item1, 0), zkStore.create(item1, WorkerUtils::withWorkerId));
  }

  @Test
  public void put() throws Exception {
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    MockAsyncStage<Stat> putAsyncStage = new MockAsyncStage<>();
    when(mockCachedModeledFramework.update(item1, 0)).thenReturn(putAsyncStage);
    putAsyncStage.complete(new Stat());
    zkStore.put(1L, Versioned.from(item1, 0));
  }

  @Test
  public void putAsync() throws Exception {
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    MockAsyncStage<Stat> putAsyncStage = new MockAsyncStage<>();
    when(mockCachedModeledFramework.update(item1, 0)).thenReturn(putAsyncStage);
    putAsyncStage.complete(new Stat());
    CompletionStage<Void> returnedAsyncStage = zkStore.putAsync(1L, Versioned.from(item1, 0));
    returnedAsyncStage.toCompletableFuture().get();
  }

  @Test
  public void putThrough() throws Exception {
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    MockAsyncStage<Stat> putAsyncStage = new MockAsyncStage<>();
    when(mockCachedModeledFramework.update(item1, 0)).thenReturn(putAsyncStage);
    putAsyncStage.complete(new Stat());
    zkStore.putThrough(1L, Versioned.from(item1, 0));
  }

  @Test
  public void remove() throws Exception {
    Mockito.when(mockModeledCache.currentChildren(ZPath.parse("/")))
        .thenReturn(
            Collections.singletonMap(
                ZPath.parse("/workers/1"), new MockZNode(item1, ZPath.parse("/workers/1"), 1)))
        .thenReturn(Collections.<ZPath, ZNode<StoredWorker>>emptyMap());
    Assert.assertEquals(1, zkStore.getAll().size());

    AsyncStage<Void> removeAsyncStage = mock(AsyncStage.class);
    when(removeAsyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.completedFuture(null));
    when(mockCachedModeledFramework.delete()).thenReturn(removeAsyncStage);
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    zkStore.remove(1L);
    Assert.assertEquals(0, zkStore.getAll().size());
  }

  @Test
  public void removeSwallowsNoNodeException() throws Exception {
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    AsyncStage<Void> removeAsyncStage = mock(AsyncStage.class);
    when(mockCachedModeledFramework.delete()).thenReturn(removeAsyncStage);
    CompletableFuture<Void> removeCompletableFuture = new CompletableFuture<>();
    removeCompletableFuture.completeExceptionally(new KeeperException.NoNodeException());
    when(removeAsyncStage.toCompletableFuture()).thenReturn(removeCompletableFuture);
    zkStore.remove(1L);
  }

  @Test(expected = ExecutionException.class)
  public void removeRethrowOtherExceptions() throws Exception {
    when(mockCachedModeledFramework.withPath(ZPath.parse("/workers/1")))
        .thenReturn(mockCachedModeledFramework);
    AsyncStage<Void> removeAsyncStage = mock(AsyncStage.class);
    when(mockCachedModeledFramework.delete()).thenReturn(removeAsyncStage);
    CompletableFuture<Void> removeCompletableFuture = new CompletableFuture<>();
    removeCompletableFuture.completeExceptionally(new KeeperException.ConnectionLossException());
    when(removeAsyncStage.toCompletableFuture()).thenReturn(removeCompletableFuture);
    zkStore.remove(1L);
  }

  private static class MockZNode<T> implements ZNode<T> {

    private final ZPath path;
    private final Stat stat;
    private final T model;

    MockZNode(T model, ZPath path, int version) {
      this.path = path;
      this.model = model;
      this.stat = new Stat();
      this.stat.setVersion(version);
    }

    @Override
    public ZPath path() {
      return path;
    }

    @Override
    public Stat stat() {
      return stat;
    }

    @Override
    public T model() {
      return model;
    }
  }
}
