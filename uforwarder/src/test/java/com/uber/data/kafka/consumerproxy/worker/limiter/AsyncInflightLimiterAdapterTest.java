package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AsyncInflightLimiterAdapterTest {
  private AsyncInflightLimiterAdapter adapter;
  private LongFixedInflightLimiter inflightLimiter;

  @BeforeEach
  public void setup() {
    inflightLimiter = new LongFixedInflightLimiter(2);
    adapter = AsyncInflightLimiterAdapter.of(inflightLimiter);
  }

  @Test
  public void testAcquireAsync() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> futurePermit1 = adapter.acquireAsync(0, 1, false);
    Assertions.assertTrue(futurePermit1.isDone());
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(0, adapter.getMetrics().getPendingQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit2 = adapter.acquireAsync(0, 2, false);
    Assertions.assertTrue(futurePermit2.isDone());
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(0, adapter.getMetrics().getPendingQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit3 = adapter.acquireAsync(0, 3, false);
    Assertions.assertFalse(futurePermit3.isDone());
    Assertions.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
    Assertions.assertTrue(futurePermit2.get().complete(InflightLimiter.Result.Succeed));
    Assertions.assertTrue(futurePermit3.isDone());
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(0, adapter.getMetrics().getPendingQueueSize());
  }

  @Test
  public void testAcquireAsyncWithMultiplePartitions()
      throws ExecutionException, InterruptedException {
    List<InflightLimiter.Permit> permits = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      permits.add(adapter.acquireAsync(0, i, false).get());
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit1 = adapter.acquireAsync(1, 1, false);
    Assertions.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit2 = adapter.acquireAsync(1, 2, false);
    Assertions.assertEquals(2, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(2, adapter.getMetrics().getPendingQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit3 = adapter.acquireAsync(2, 1, false);
    Assertions.assertEquals(3, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(3, adapter.getMetrics().getPendingQueueSize());
    for (InflightLimiter.Permit permit : permits) {
      permit.complete();
    }
    // round-robin between partitions, in order with-in partition
    Assertions.assertTrue(futurePermit3.isDone());
    Assertions.assertTrue(futurePermit1.isDone());
    Assertions.assertFalse(futurePermit2.isDone());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
    // complete last one
    futurePermit1.get().complete();
    Assertions.assertTrue(futurePermit2.isDone());
  }

  @Test
  public void testCloseRelease() {
    List<CompletableFuture<InflightLimiter.Permit>> permits = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      permits.add(adapter.acquireAsync(0, 1, false));
    }
    int done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assertions.assertEquals(2, done);
    inflightLimiter.close();
    adapter.close();
    done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assertions.assertEquals(3, done);
  }

  @Test
  public void testAcquireSucceedAndCancel() {
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync(0, 1, false);
    Assertions.assertFalse(futurePermit.cancel(false));
  }

  @Test
  public void testAcquireFailedAndCancel() throws ExecutionException, InterruptedException {
    for (int i = 0; i < 2; ++i) {
      adapter.acquireAsync(0, i, false);
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync(0, 2, false);
    Assertions.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
    futurePermit.cancel(false);
    Assertions.assertTrue(futurePermit.isDone());
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
  }

  @Test
  public void testCompletePermitDequeFuturePermit()
      throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = null;
    for (int i = 0; i < 2; ++i) {
      acquiredPermit = adapter.acquireAsync(0, i, false);
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync(0, 1, false);
    Assertions.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed);
    Assertions.assertTrue(futurePermit.isDone());
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(0, adapter.getMetrics().getPendingQueueSize());
  }

  @Test
  public void testCancelCompletedFuture() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = new CompletableFuture();
    acquiredPermit.cancel(false);
    Assertions.assertTrue(acquiredPermit.isCancelled());
    Assertions.assertFalse(acquiredPermit.complete(InflightLimiter.NoopPermit.INSTANCE));
    Assertions.assertTrue(acquiredPermit.isDone());
    Assertions.assertTrue(acquiredPermit.isCancelled());
    Assertions.assertTrue(acquiredPermit.isCompletedExceptionally());
  }

  @Test
  public void testCompletePermitCompletableFuture() {
    for (int i = 0; i < 2; ++i) {
      adapter.acquireAsync(0, i, false);
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync(0, 2, false);
    Assertions.assertFalse(futurePermit.isDone());
    futurePermit.completeExceptionally(new CancellationException());
    AtomicBoolean complected = new AtomicBoolean(false);
    futurePermit.complete(
        new InflightLimiter.AbstractPermit() {
          @Override
          protected boolean doComplete(InflightLimiter.Result result) {
            return complected.compareAndSet(false, true);
          }
        });
    Assertions.assertEquals(true, complected.get());
  }

  @Test
  public void testCompletePermitDequeCompletedFuturePermit()
      throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = null;
    for (int i = 0; i < 2; ++i) {
      acquiredPermit = adapter.acquireAsync(0, i, false);
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync(1, 0, false);
    Assertions.assertFalse(futurePermit.isDone());
    futurePermit.completeExceptionally(new CancellationException()); // complete a future permit
    Assertions.assertTrue(futurePermit.isDone());
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(0, inflightLimiter.getMetrics().availablePermits());
    Assertions.assertEquals(1, adapter.getMetrics().getPendingQueueSize());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed); // it should return permit
    Assertions.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assertions.assertEquals(1, inflightLimiter.getMetrics().availablePermits());
    Assertions.assertEquals(0, adapter.getMetrics().getPendingQueueSize());
  }
}
