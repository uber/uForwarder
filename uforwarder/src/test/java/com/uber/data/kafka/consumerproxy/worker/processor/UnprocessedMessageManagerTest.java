package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.config.ProcessorConfiguration;
import com.uber.data.kafka.consumerproxy.worker.limiter.LongFixedInflightLimiter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class UnprocessedMessageManagerTest extends ProcessorTestBase {
  private TopicPartition tp1;
  private TopicPartition tp2;
  private TopicPartition tp3;
  private TopicPartition tp4;
  private TopicPartitionOffset physicalKafkaMetadata1;
  private TopicPartitionOffset physicalKafkaMetadata2;
  private TopicPartitionOffset physicalKafkaMetadata3;
  private TopicPartitionOffset physicalKafkaMetadata4;
  private ProcessorMessage pm1;
  private ProcessorMessage pm2;
  private ProcessorMessage pm3;
  private ProcessorMessage pm4;
  private UnprocessedMessageManager unprocessedMessageManager;
  private LongFixedInflightLimiter sharedByteSizeLimiter;
  private Job job1;
  private Job job2;
  private Job job4;
  private Gauge mockCountGauge;
  private Gauge mockBytesGauge;
  private Gauge mockGlobalBytesGauge;
  private Scope mockScope;
  private ProcessorConfiguration config;

  @Before
  public void setUp() throws Exception {
    mockScope = Mockito.mock(Scope.class);
    mockCountGauge = Mockito.mock(Gauge.class);
    mockBytesGauge = Mockito.mock(Gauge.class);
    mockGlobalBytesGauge = Mockito.mock(Gauge.class);
    Counter counter = Mockito.mock(Counter.class);
    Mockito.when(mockScope.tagged(Mockito.anyMap())).thenReturn(mockScope);
    Mockito.when(mockScope.gauge(UnprocessedMessageManager.MetricNames.PROCESSOR_MESSAGES_COUNT))
        .thenReturn(mockCountGauge);
    Mockito.when(mockScope.gauge(UnprocessedMessageManager.MetricNames.PROCESSOR_MESSAGES_BYTES))
        .thenReturn(mockBytesGauge);
    Mockito.when(
            mockScope.gauge(UnprocessedMessageManager.MetricNames.PROCESSOR_MESSAGES_GLOBAL_BYTES))
        .thenReturn(mockGlobalBytesGauge);
    Mockito.when(mockScope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    tp1 = new TopicPartition("topic1", 1);
    tp2 = new TopicPartition("topic1", 2);
    tp3 = new TopicPartition("topic1", 3);
    tp4 = new TopicPartition("topic1", 4);
    job1 = newJob("topic1", 1);
    job2 = newJob("topic1", 2);
    job4 = newJob("topic1", 4);
    physicalKafkaMetadata1 = new TopicPartitionOffset(tp1.topic(), tp1.partition(), 1);
    physicalKafkaMetadata2 = new TopicPartitionOffset(tp2.topic(), tp2.partition(), 1);
    physicalKafkaMetadata3 = new TopicPartitionOffset(tp3.topic(), tp3.partition(), 1);
    physicalKafkaMetadata4 = new TopicPartitionOffset(tp4.topic(), tp4.partition(), 1);
    pm1 = newProcessMessage(physicalKafkaMetadata1);
    pm2 = newProcessMessage(physicalKafkaMetadata2);
    pm3 = newProcessMessage(physicalKafkaMetadata3);
    pm4 = newEmptyProcessMessage(physicalKafkaMetadata4);
    sharedByteSizeLimiter = new LongFixedInflightLimiter(10);
    config = new ProcessorConfiguration();
    config.setMaxInboundCacheCount(2);
    config.setMaxInboundCacheByteSize(8);
    unprocessedMessageManager =
        new UnprocessedMessageManager(job1, config, sharedByteSizeLimiter, mockScope);
    unprocessedMessageManager.init(job1);
    unprocessedMessageManager.init(job2);
    unprocessedMessageManager.init(job4);
  }

  @Test
  public void testGetLimiter() {
    UnprocessedMessageManager.PartitionLimiter limiter = unprocessedMessageManager.getLimiter(tp3);
    Assert.assertNull(limiter);
  }

  @Test
  public void testCancel() {
    // cancel tp3
    unprocessedMessageManager.cancel(tp2);
    unprocessedMessageManager.cancel(tp3);
    unprocessedMessageManager.cancel(tp4);

    IllegalStateException exception = null;
    UnprocessedMessageManager.PartitionLimiter limiter = unprocessedMessageManager.getLimiter(tp1);
    unprocessedMessageManager.cancel(tp1);
    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertTrue(limiter.isClosed());

    try {
      unprocessedMessageManager.receive(pm1);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    // cancel tp1 again
    unprocessedMessageManager.cancel(tp1);

    // cancel tp2
    exception = null;
    unprocessedMessageManager.cancel(tp2);
    try {
      unprocessedMessageManager.receive(pm2);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  @Test
  public void testJobShareLimit() throws InterruptedException {
    Assert.assertEquals(6, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    unprocessedMessageManager.getLimiter(tp1).acquire(pm1);
    unprocessedMessageManager.getLimiter(tp2).acquire(pm2);
    Assert.assertEquals(4, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
  }

  @Test
  public void testCancelUnblockReceive() throws Exception {
    Assert.assertEquals(6, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    UnprocessedMessageManager.PartitionLimiter limiter1 = unprocessedMessageManager.getLimiter(tp1);
    List<ProcessorMessage> pms = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      pm1 = newProcessMessage(physicalKafkaMetadata1);
      pms.add(pm1);
      unprocessedMessageManager.receive(pm1);
      unprocessedMessageManager.receive(pm2);
    }
    AtomicReference<Throwable> error = new AtomicReference(null);
    CompletableFuture<Void> acquiredFuture =
        CompletableFuture.runAsync(
            () -> {
              try {
                pm1 = newProcessMessage(physicalKafkaMetadata1);
                pms.add(pm1);
                unprocessedMessageManager.receive(pm1);
              } catch (Exception e) {
                error.set(e);
              }
            });

    try {
      acquiredFuture.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
    }

    Assert.assertFalse(acquiredFuture.isDone());
    // cancel partition  2 reduce limit by 2 but inflight by 3
    unprocessedMessageManager.cancel(tp2);
    acquiredFuture.get();
    Assert.assertNull(error.get());
    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertEquals(4, unprocessedMessageManager.countLimiter.getMetrics().getInflight());
    for (ProcessorMessage pm : pms) {
      unprocessedMessageManager.remove(pm);
    }
    Assert.assertEquals(4, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().getInflight());
  }

  @Test
  public void testCancelBlockingJob() throws Exception {
    List<ProcessorMessage> pms = new ArrayList<>();
    for (int i = 0; i < 6; ++i) {
      pm1 = newProcessMessage(physicalKafkaMetadata1);
      pms.add(pm1);
      unprocessedMessageManager.receive(pm1);
    }

    AtomicReference<Throwable> error = new AtomicReference(null);
    CompletableFuture<Void> acquiredFuture =
        CompletableFuture.runAsync(
            () -> {
              try {
                pm1 = newProcessMessage(physicalKafkaMetadata1);
                pms.add(pm1);
                unprocessedMessageManager.receive(pm1);
              } catch (Exception e) {
                error.set(e);
              }
            });

    try {
      acquiredFuture.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
    }
    unprocessedMessageManager.cancel(tp1);
    acquiredFuture.get();
    Assert.assertNull(error.get());
  }

  @Test
  public void testAssignUnblock() throws Exception {
    unprocessedMessageManager.cancelAll();
    unprocessedMessageManager.init(job1);
    List<ProcessorMessage> pms = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      pm1 = newProcessMessage(physicalKafkaMetadata1);
      pms.add(pm1);
      unprocessedMessageManager.receive(pm1);
    }
    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    AtomicReference<Throwable> error = new AtomicReference(null);
    CompletableFuture<Void> acquiredFuture =
        CompletableFuture.runAsync(
            () -> {
              try {
                pm1 = newProcessMessage(physicalKafkaMetadata1);
                pms.add(pm1);
                unprocessedMessageManager.receive(pm1);
              } catch (Exception e) {
                error.set(e);
              }
            });
    try {
      acquiredFuture.get(100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
    }
    unprocessedMessageManager.init(job2);
    acquiredFuture.get();
    Assert.assertEquals(1, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
  }

  @Test
  public void testCancelReleaseGlobalPermits() {
    unprocessedMessageManager.cancel(tp2);
    unprocessedMessageManager.cancel(tp4);
    unprocessedMessageManager.receive(pm1);
    Assert.assertEquals(0, sharedByteSizeLimiter.getMetrics().getInflight());
    unprocessedMessageManager.receive(pm1);
    Assert.assertEquals(5, sharedByteSizeLimiter.getMetrics().getInflight());
    unprocessedMessageManager.cancel(tp1);
    Assert.assertEquals(0, sharedByteSizeLimiter.getMetrics().getInflight());
  }

  @Test
  public void testCancelAll() {
    IllegalStateException exception = null;

    UnprocessedMessageManager.PartitionLimiter limiter1 = unprocessedMessageManager.getLimiter(tp1);
    UnprocessedMessageManager.PartitionLimiter limiter2 = unprocessedMessageManager.getLimiter(tp2);
    unprocessedMessageManager.cancelAll();
    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertTrue(limiter1.isClosed());
    Assert.assertTrue(limiter2.isClosed());

    try {
      unprocessedMessageManager.receive(pm1);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    unprocessedMessageManager.remove(pm1);
  }

  @Test
  public void testReceive() {
    Assert.assertEquals(
        6, unprocessedMessageManager.topicPartitionLimiterMap.get(tp1).countLimit());
    unprocessedMessageManager.cancel(tp2);
    unprocessedMessageManager.cancel(tp4);
    Assert.assertEquals(
        2, unprocessedMessageManager.topicPartitionLimiterMap.get(tp1).countLimit());
    Assert.assertEquals(
        0, unprocessedMessageManager.countLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(2, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertEquals(0, sharedByteSizeLimiter.getMetrics().getInflight());
    Assert.assertEquals(0, unprocessedMessageManager.byteSizeLimiter.getMetrics().getInflight());
    unprocessedMessageManager.receive(pm1);
    Assert.assertEquals(
        0, unprocessedMessageManager.countLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(1, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertEquals(0, sharedByteSizeLimiter.getMetrics().getInflight());
    Assert.assertEquals(5, unprocessedMessageManager.byteSizeLimiter.getMetrics().getInflight());
    unprocessedMessageManager.receive(pm1);
    Assert.assertEquals(
        0, unprocessedMessageManager.countLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertEquals(5, sharedByteSizeLimiter.getMetrics().getInflight());
    Assert.assertEquals(5, unprocessedMessageManager.byteSizeLimiter.getMetrics().getInflight());

    IllegalStateException exception = null;
    try {
      unprocessedMessageManager.receive(pm3);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  @Test
  public void testReceiveEmptyMessage() throws InterruptedException {
    IllegalStateException exception = null;
    try {
      unprocessedMessageManager.receive(pm4);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNull(exception);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 1000)
  public void testReceiveMore() {
    unprocessedMessageManager.cancel(tp2);
    unprocessedMessageManager.cancel(tp4);
    unprocessedMessageManager.receive(pm1);
    unprocessedMessageManager.receive(pm1);
    AtomicBoolean tried = new AtomicBoolean(false);

    new Thread(
            () -> {
              tried.set(true);
              unprocessedMessageManager.receive(pm1);
            })
        .start();

    do {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (!tried.get());

    Assert.assertEquals(0, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
    Assert.assertEquals(5, sharedByteSizeLimiter.getMetrics().getInflight());
    Assert.assertEquals(5, unprocessedMessageManager.byteSizeLimiter.getMetrics().getInflight());

    // the getQueueLength will eventually return 1
    while (unprocessedMessageManager.countLimiter.getMetrics().getBlockingQueueSize() != 1) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    Assert.assertEquals(
        1, unprocessedMessageManager.countLimiter.getMetrics().getBlockingQueueSize());
  }

  @Test
  public void testRemove() {
    unprocessedMessageManager.remove(pm1);
    Assert.assertEquals(6, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());

    unprocessedMessageManager.remove(pm3);
  }

  @Test
  public void testDetectBlockingAndMarkCancel() {
    Assert.assertFalse(unprocessedMessageManager.detectBlockingMessage(tp1).isPresent());
    Assert.assertFalse(unprocessedMessageManager.markCanceled(physicalKafkaMetadata1));
  }

  @Test
  public void testWithProcessorInboundCacheLimit() {
    config.setMaxProcessorInBoundCacheCount(3);
    unprocessedMessageManager =
        new UnprocessedMessageManager(job1, config, sharedByteSizeLimiter, mockScope);
    unprocessedMessageManager.init(job1);
    unprocessedMessageManager.init(job2);
    Assert.assertEquals(3, unprocessedMessageManager.countLimiter.getMetrics().availablePermits());
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 30000)
  public void testReceiveAndRemove() {
    AtomicBoolean reached = new AtomicBoolean(false);
    unprocessedMessageManager.cancel(tp2);
    unprocessedMessageManager.cancel(tp4);
    unprocessedMessageManager.receive(pm1);
    unprocessedMessageManager.receive(pm1);
    CompletableFuture<Void> received =
        CompletableFuture.runAsync(
            () -> {
              unprocessedMessageManager.receive(pm1);
              reached.set(true);
            });

    CompletableFuture.runAsync(() -> unprocessedMessageManager.remove(pm1));

    received.join();
    Assert.assertTrue(reached.get());
  }

  @Test
  public void testReceiveMetrics() {
    ArgumentCaptor<Double> countCaptor = ArgumentCaptor.forClass(Double.class);
    ArgumentCaptor<Double> bytesCaptor = ArgumentCaptor.forClass(Double.class);
    unprocessedMessageManager.cancelAll();
    unprocessedMessageManager.init(job);
    unprocessedMessageManager.receive(processorMessage);
    unprocessedMessageManager.receive(processorMessage);
    unprocessedMessageManager.publishMetrics();
    Mockito.verify(mockCountGauge).update(countCaptor.capture());
    Mockito.verify(mockBytesGauge).update(bytesCaptor.capture());
    Assert.assertEquals(2, countCaptor.getValue(), 0.001);
    Assert.assertEquals(10, bytesCaptor.getValue(), 0.001);
  }

  @Test
  public void testRemoveMetrics() {
    ArgumentCaptor<Double> countCaptor = ArgumentCaptor.forClass(Double.class);
    ArgumentCaptor<Double> bytesCaptor = ArgumentCaptor.forClass(Double.class);
    unprocessedMessageManager.cancelAll();
    unprocessedMessageManager.init(job);
    unprocessedMessageManager.receive(processorMessage);
    unprocessedMessageManager.remove(processorMessage);
    unprocessedMessageManager.publishMetrics();
    Mockito.verify(mockCountGauge).update(countCaptor.capture());
    Mockito.verify(mockBytesGauge).update(bytesCaptor.capture());
    Assert.assertEquals(0, countCaptor.getValue(), 0.001);
    Assert.assertEquals(0, bytesCaptor.getValue(), 0.001);
  }

  @Test
  public void testCancelMetrics() {
    unprocessedMessageManager.cancelAll();
    unprocessedMessageManager.init(job);
    unprocessedMessageManager.receive(processorMessage);
    unprocessedMessageManager.cancel(new TopicPartition(TOPIC, PARTITION));
    unprocessedMessageManager.publishMetrics();
    Mockito.verify(mockCountGauge, Mockito.never()).update(Mockito.anyDouble());
    Mockito.verify(mockBytesGauge, Mockito.never()).update(Mockito.anyDouble());
  }

  @Test
  public void testCancelAllMetrics() {
    unprocessedMessageManager.cancelAll();
    unprocessedMessageManager.init(job);
    unprocessedMessageManager.receive(processorMessage);
    unprocessedMessageManager.cancelAll();
    unprocessedMessageManager.publishMetrics();
    Mockito.verify(mockCountGauge, Mockito.never()).update(Mockito.anyDouble());
    Mockito.verify(mockBytesGauge, Mockito.never()).update(Mockito.anyDouble());
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testReceiveAndInterrupt() {
    unprocessedMessageManager.receive(pm1);
    unprocessedMessageManager.receive(pm1);
    Thread thread =
        new Thread(
            () -> {
              unprocessedMessageManager.receive(pm1);
            });
    thread.start();

    new Thread(
            () -> {
              try {
                Thread.sleep(5);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              thread.interrupt();
            })
        .start();

    try {
      thread.join(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
