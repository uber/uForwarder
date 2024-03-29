package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TTLDecoratorTest extends FievelTestBase {
  private Store<Long, StoredWorker> workerStore;
  private TTLDecorator<Long, StoredWorker> ttlWorkerStore;
  private ScheduledExecutorService executorService;
  private ConcurrentMap<Long, ScheduledFuture> expirationTaskMap;
  private LeaderSelector leaderSelector;
  private Versioned<StoredWorker> itemOne;
  private CoreInfra coreInfra;

  @Before
  public void setup() {
    coreInfra = CoreInfra.NOOP;
    itemOne =
        VersionedProto.from(
            StoredWorker.newBuilder().setNode(Node.newBuilder().setId(1).build()).build());
    workerStore = Mockito.mock(Store.class);
    executorService = Mockito.mock(ScheduledExecutorService.class);
    expirationTaskMap = new ConcurrentHashMap<>();
    leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);

    ttlWorkerStore =
        new TTLDecorator<>(
            coreInfra,
            workerStore,
            w -> w.getNode().getId(),
            executorService,
            expirationTaskMap,
            Duration.ofSeconds(2),
            leaderSelector);
    ttlWorkerStore.start();
  }

  @After
  public void teardown() {
    ttlWorkerStore.stop();
  }

  @Test
  public void testUpdateTTLScheduledExpirationTask() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.doReturn(scheduledFuture)
        .when(executorService)
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
    ttlWorkerStore.updateTTL(itemOne);
    Assert.assertEquals(1, expirationTaskMap.size());
    Assert.assertEquals(scheduledFuture, expirationTaskMap.get(1L));
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testUpdateTTLCancelsOldExpirationTask() throws Exception {
    ScheduledFuture firstScheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.doReturn(firstScheduledFuture)
        .when(executorService)
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
    ttlWorkerStore.updateTTL(itemOne);
    Assert.assertEquals(1, expirationTaskMap.size());
    Assert.assertEquals(firstScheduledFuture, expirationTaskMap.get(1L));
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());

    // Call updateTTL again should override the old item in the scheduled task map
    // And old scheduled future should be canceled.
    Mockito.when(firstScheduledFuture.cancel(false)).thenReturn(true);
    ScheduledFuture secondScheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.doReturn(secondScheduledFuture)
        .when(executorService)
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
    ttlWorkerStore.updateTTL(itemOne);
    Assert.assertEquals(1, expirationTaskMap.size());
    Assert.assertEquals(secondScheduledFuture, expirationTaskMap.get(1L));
    Mockito.verify(executorService, Mockito.times(2))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
    Mockito.verify(firstScheduledFuture, Mockito.times(1)).cancel(false);
  }

  @Test
  public void testExpirationTaskExpires() throws Exception {
    Consumer<TTLDecorator.ExpirationTask> rescheduleFunc = Mockito.mock(Consumer.class);
    TTLDecorator.ExpirationTask expirationTask =
        new TTLDecorator.ExpirationTask(
            coreInfra, workerStore, rescheduleFunc, 1L, leaderSelector, 0);
    expirationTask.run();
    ArgumentCaptor<String> runnableNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(leaderSelector, Mockito.atLeastOnce())
        .runIfLeader(runnableNameCaptor.capture(), runnableCaptor.capture());
    runnableCaptor.getValue().run();
    Mockito.verify(workerStore, Mockito.times(1)).remove(1L);
  }

  @Test
  public void testExpirationTaskFailureReschedulesTask() throws Exception {
    Mockito.doThrow(new Exception()).when(workerStore).remove(Mockito.anyLong());
    Consumer<TTLDecorator.ExpirationTask> rescheduleFunc = Mockito.mock(Consumer.class);
    TTLDecorator.ExpirationTask expirationTask =
        new TTLDecorator.ExpirationTask(
            coreInfra, workerStore, rescheduleFunc, 1L, leaderSelector, 0);
    expirationTask.run();
    ArgumentCaptor<String> runnableNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(leaderSelector, Mockito.atLeastOnce())
        .runIfLeader(runnableNameCaptor.capture(), runnableCaptor.capture());
    runnableCaptor.getValue().run();
    ArgumentCaptor<TTLDecorator.ExpirationTask> argumentCaptor =
        ArgumentCaptor.forClass(TTLDecorator.ExpirationTask.class);
    Mockito.verify(rescheduleFunc, Mockito.times(1)).accept(argumentCaptor.capture());
    Assert.assertEquals(1, argumentCaptor.getValue().getAttempt());
  }

  @Test
  public void testExpirationSkippedIfNotLeader() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    Consumer<TTLDecorator.ExpirationTask> rescheduleFunc = Mockito.mock(Consumer.class);
    TTLDecorator.ExpirationTask expirationTask =
        new TTLDecorator.ExpirationTask(
            coreInfra, workerStore, rescheduleFunc, 1L, leaderSelector, 0);
    expirationTask.run();
    Mockito.verify(workerStore, Mockito.times(0)).remove(Mockito.anyLong());
  }

  @Test
  public void testGet() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);
    Mockito.when(workerStore.get(Mockito.anyLong())).thenReturn(itemOne);
    Assert.assertEquals(itemOne, ttlWorkerStore.get(1L));
    Mockito.verify(workerStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testGetThrough() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);
    Mockito.when(workerStore.getThrough(Mockito.anyLong())).thenReturn(itemOne);
    Assert.assertEquals(itemOne, ttlWorkerStore.getThrough(1L));
    Mockito.verify(workerStore, Mockito.times(1)).getThrough(Mockito.anyLong());
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testCreate() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);
    Mockito.when(workerStore.create(Mockito.any(), Mockito.any())).thenReturn(itemOne);
    Assert.assertEquals(itemOne, ttlWorkerStore.create(itemOne.model(), (k, v) -> v));
    Mockito.verify(workerStore, Mockito.times(1)).create(Mockito.any(), Mockito.any());
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testPut() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);
    ttlWorkerStore.put(1L, itemOne);
    Mockito.verify(workerStore, Mockito.times(1)).put(Mockito.any(), Mockito.any());
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testPutAsync() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);
    CompletableFuture putAsyncFuture = CompletableFuture.completedFuture(null);
    Mockito.when(workerStore.putAsync(Mockito.anyLong(), Mockito.any())).thenReturn(putAsyncFuture);
    ttlWorkerStore.putAsync(1L, itemOne);
    Mockito.verify(workerStore, Mockito.times(1)).putAsync(Mockito.any(), Mockito.any());
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testPutThrough() throws Exception {
    ScheduledFuture scheduledFuture = Mockito.mock(ScheduledFuture.class);
    Mockito.when(
            executorService.schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any()))
        .thenReturn(scheduledFuture);
    ttlWorkerStore.putThrough(1L, itemOne);
    Mockito.verify(workerStore, Mockito.times(1)).putThrough(Mockito.any(), Mockito.any());
    Mockito.verify(executorService, Mockito.times(1))
        .schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any());
  }

  @Test
  public void testLeaderChange() throws Exception {
    Mockito.when(workerStore.getAll()).thenReturn(ImmutableMap.of(1L, itemOne));
    ttlWorkerStore.isLeader();
    Mockito.verify(workerStore, Mockito.atLeastOnce()).getAll();
    Assert.assertEquals(1, expirationTaskMap.size());

    ttlWorkerStore.notLeader();

    // exception should be caught and handled.
    Mockito.doThrow(new RuntimeException()).when(workerStore).getAll();
    ttlWorkerStore.isLeader();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUpdateTTLWithNegativeTTL() {
    new TTLDecorator<>(
        coreInfra,
        workerStore,
        w -> w.getNode().getId(),
        executorService,
        expirationTaskMap,
        Duration.ZERO,
        leaderSelector);
  }

  @Test
  public void testIsRunning() {
    Mockito.when(workerStore.isRunning()).thenReturn(true);
    Assert.assertTrue(ttlWorkerStore.isRunning());
  }

  @Test
  public void testInitializedIsLeader() throws Exception {
    CompletableFuture<Map<Long, Versioned<StoredWorker>>> initializedFuture =
        new CompletableFuture<>();
    Mockito.when(workerStore.initialized()).thenReturn(initializedFuture);
    initializedFuture.complete(ImmutableMap.of(1L, itemOne));

    CompletableFuture<Map<Long, Versioned<StoredWorker>>> ttlFuture = ttlWorkerStore.initialized();

    Assert.assertTrue(ttlFuture.isDone());
    Assert.assertEquals(1, expirationTaskMap.size());
  }

  @Test
  public void testInitializedIsNotLeader() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);

    CompletableFuture<Map<Long, Versioned<StoredWorker>>> initializedFuture =
        new CompletableFuture<>();
    Mockito.when(workerStore.initialized()).thenReturn(initializedFuture);
    initializedFuture.complete(ImmutableMap.of(1L, itemOne));

    CompletableFuture<Map<Long, Versioned<StoredWorker>>> ttlFuture = ttlWorkerStore.initialized();

    Assert.assertTrue(ttlFuture.isDone());
    Assert.assertEquals(0, expirationTaskMap.size());
  }

  @Test
  public void testGetAll() throws Exception {
    Map<Long, Versioned<StoredWorker>> workerMap = ImmutableMap.of(1L, itemOne);
    Mockito.when(workerStore.getAll()).thenReturn(workerMap);
    Mockito.when(workerStore.getAll(Mockito.any())).thenReturn(workerMap);
    Assert.assertEquals(1, ttlWorkerStore.getAll().size());
    Assert.assertEquals(1, ttlWorkerStore.getAll(w -> true).size());
  }

  @Test
  public void testRemove() throws Exception {
    ttlWorkerStore.remove(1L);
    Mockito.verify(workerStore, Mockito.times(1)).remove(1L);
  }
}
