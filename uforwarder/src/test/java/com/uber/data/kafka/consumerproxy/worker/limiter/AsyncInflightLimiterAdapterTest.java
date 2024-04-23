package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncInflightLimiterAdapterTest extends FievelTestBase {
  private AsyncInflightLimiterAdapter adapter;
  private LongFixedInflightLimiter inflightLimiter;

  @Before
  public void setup() {
    inflightLimiter = new LongFixedInflightLimiter(2);
    adapter = AsyncInflightLimiterAdapter.of(inflightLimiter);
  }

  @Test
  public void testAcquireAsync() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> futurePermit1 = adapter.acquireAsync();
    Assert.assertTrue(futurePermit1.isDone());
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit2 = adapter.acquireAsync();
    Assert.assertTrue(futurePermit2.isDone());
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit3 = adapter.acquireAsync();
    Assert.assertFalse(futurePermit3.isDone());
    Assert.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    Assert.assertTrue(futurePermit2.get().complete(InflightLimiter.Result.Succeed));
    Assert.assertTrue(futurePermit3.isDone());
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCloseRelease() {
    List<CompletableFuture<InflightLimiter.Permit>> permits = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      permits.add(adapter.acquireAsync());
    }
    int done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assert.assertEquals(2, done);
    inflightLimiter.close();
    adapter.close();
    done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assert.assertEquals(3, done);
  }

  @Test
  public void testAcquireSucceedAndCancel() {
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync();
    Assert.assertFalse(futurePermit.cancel(false));
  }

  @Test
  public void testAcquireFailedAndCancel() throws ExecutionException, InterruptedException {
    for (int i = 0; i < 2; ++i) {
      adapter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync();
    Assert.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    futurePermit.cancel(false);
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCompletePermitDequeFuturePermit()
      throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = null;
    for (int i = 0; i < 2; ++i) {
      acquiredPermit = adapter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync();
    Assert.assertEquals(1, adapter.getMetrics().getAsyncQueueSize());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed);
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCancelCompletedFuture() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = new CompletableFuture();
    acquiredPermit.cancel(false);
    Assert.assertTrue(acquiredPermit.isCancelled());
    Assert.assertFalse(acquiredPermit.complete(InflightLimiter.NoopPermit.INSTANCE));
    Assert.assertTrue(acquiredPermit.isDone());
    Assert.assertTrue(acquiredPermit.isCancelled());
    Assert.assertTrue(acquiredPermit.isCompletedExceptionally());
  }

  @Test
  public void testCompletePermitCompletableFuture() {
    for (int i = 0; i < 2; ++i) {
      adapter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync();
    Assert.assertFalse(futurePermit.isDone());
    futurePermit.completeExceptionally(new CancellationException());
    AtomicBoolean complected = new AtomicBoolean(false);
    futurePermit.complete(
        new InflightLimiter.AbstractPermit() {
          @Override
          protected boolean doComplete(InflightLimiter.Result result) {
            return complected.compareAndSet(false, true);
          }
        });
    Assert.assertEquals(true, complected.get());
  }

  @Test
  public void testCompletePermitDequeCompletedFuturePermit()
      throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = null;
    for (int i = 0; i < 2; ++i) {
      acquiredPermit = adapter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adapter.acquireAsync();
    Assert.assertFalse(futurePermit.isDone());
    futurePermit.completeExceptionally(new CancellationException()); // complete a future permit
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assert.assertEquals(0, inflightLimiter.getMetrics().availablePermits());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed); // it should return permit
    Assert.assertEquals(0, adapter.getMetrics().getAsyncQueueSize());
    Assert.assertEquals(1, inflightLimiter.getMetrics().availablePermits());
  }
}
