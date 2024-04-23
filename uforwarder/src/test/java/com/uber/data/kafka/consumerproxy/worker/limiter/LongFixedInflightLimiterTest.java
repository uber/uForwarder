package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LongFixedInflightLimiterTest extends FievelTestBase {

  private LongFixedInflightLimiter inflightLimiter;

  @Before
  public void setUp() {
    inflightLimiter = new LongFixedInflightLimiter(2);
  }

  @Test
  public void testAcquire() throws InterruptedException {
    Assert.assertEquals(0, inflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getInflight());
    Assert.assertEquals(2, inflightLimiter.getMetrics().getLimit());
    inflightLimiter.acquire();
    Assert.assertEquals(0, inflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(1, inflightLimiter.getMetrics().getInflight());
    Assert.assertEquals(2, inflightLimiter.getMetrics().getLimit());
    inflightLimiter.acquire();
    Assert.assertEquals(0, inflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(2, inflightLimiter.getMetrics().getInflight());
    Assert.assertEquals(2, inflightLimiter.getMetrics().getLimit());
  }

  @Test
  public void testAcquireMore() throws InterruptedException {
    inflightLimiter.acquire(2);
    CompletableFuture.runAsync(
        () -> {
          try {
            inflightLimiter.acquire();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

    Assert.assertEquals(2, inflightLimiter.getMetrics().getInflight());
    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> Assert.assertEquals(1, inflightLimiter.getMetrics().getBlockingQueueSize()));
  }

  @Test
  public void testRelease() throws InterruptedException {
    InflightLimiter.Permit permit = inflightLimiter.acquire();
    Assert.assertEquals(0, inflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(1, inflightLimiter.getMetrics().getInflight());
    permit.complete(InflightLimiter.Result.Succeed);
    Assert.assertEquals(0, inflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getInflight());
  }

  @Test(timeout = 1000)
  public void testAcquireAndUpdateLimit() throws InterruptedException {
    Thread t1 =
        new Thread(
            () -> {
              try {
                inflightLimiter.acquire(3);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    t1.start();

    new Thread(
            () -> {
              try {
                inflightLimiter.updateLimit(3);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .start();
    t1.join(1000);
  }

  @Test(timeout = 1000)
  public void testAcquireAndRelease() throws InterruptedException {
    AtomicBoolean reached = new AtomicBoolean(false);
    inflightLimiter.acquire();
    InflightLimiter.Permit permit = inflightLimiter.acquire();
    Thread thread =
        new Thread(
            () -> {
              try {
                inflightLimiter.acquire();
                reached.set(true);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    thread.start();

    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> inflightLimiter.getMetrics().getBlockingQueueSize() == 1);
    permit.complete(InflightLimiter.Result.Succeed);

    try {
      thread.join(1000);
    } catch (InterruptedException e) {
      Assert.assertTrue(false);
    }

    Assert.assertTrue(reached.get());
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testAcquireAndInterrupt() throws InterruptedException {
    inflightLimiter.acquire(2);
    Thread thread =
        new Thread(
            () -> {
              try {
                inflightLimiter.acquire();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
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

  @Test
  public void testUpdateLimit() throws Exception {
    inflightLimiter.updateLimit(0);
    Assert.assertEquals(0, inflightLimiter.getMetrics().getLimit());

    inflightLimiter.updateLimit(4);
    Assert.assertEquals(4, inflightLimiter.getMetrics().getLimit());

    inflightLimiter.updateLimit(3);
    Assert.assertEquals(3, inflightLimiter.getMetrics().getLimit());

    inflightLimiter.updateLimit(-1);
    Assert.assertEquals(0, inflightLimiter.getMetrics().getLimit());
  }

  @Test
  public void testAcquireWithZeroLimit() throws InterruptedException {
    inflightLimiter.updateLimit(0);
    Assert.assertEquals(0, inflightLimiter.getMetrics().getLimit());

    InflightLimiter.Permit permit = inflightLimiter.acquire();
    Assert.assertEquals(0, inflightLimiter.getMetrics().getInflight());
    permit.complete(InflightLimiter.Result.Succeed);
  }

  @Test
  public void testTryAcquire() {
    Optional<InflightLimiter.Permit> permit1 = inflightLimiter.tryAcquire();
    Assert.assertTrue(permit1.isPresent());
    Optional<InflightLimiter.Permit> permit2 = inflightLimiter.tryAcquire();
    Assert.assertTrue(permit2.isPresent());
    Optional<InflightLimiter.Permit> permit3 = inflightLimiter.tryAcquire();
    Assert.assertFalse(permit3.isPresent());
    permit1.get().complete(InflightLimiter.Result.Succeed);
    permit3 = inflightLimiter.tryAcquire();
    Assert.assertTrue(permit3.isPresent());
  }

  @Test
  public void testAcquireAsync() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> futurePermit1 = inflightLimiter.acquireAsync();
    Assert.assertTrue(futurePermit1.isDone());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit2 = inflightLimiter.acquireAsync();
    Assert.assertTrue(futurePermit2.isDone());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit3 = inflightLimiter.acquireAsync();
    Assert.assertFalse(futurePermit3.isDone());
    Assert.assertEquals(1, inflightLimiter.getMetrics().getAsyncQueueSize());
    Assert.assertTrue(futurePermit2.get().complete(InflightLimiter.Result.Succeed));
    Assert.assertTrue(futurePermit3.isDone());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCloseRelease() {
    List<CompletableFuture<InflightLimiter.Permit>> permits = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      permits.add(inflightLimiter.acquireAsync());
    }
    int done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assert.assertEquals(2, done);
    inflightLimiter.close();
    done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assert.assertEquals(3, done);
  }

  @Test
  public void testAcquireSucceedAndCancel() {
    CompletableFuture<InflightLimiter.Permit> futurePermit = inflightLimiter.acquireAsync();
    Assert.assertFalse(futurePermit.cancel(false));
  }

  @Test
  public void testAcquireFailedAndCancel() throws ExecutionException, InterruptedException {
    for (int i = 0; i < 2; ++i) {
      inflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = inflightLimiter.acquireAsync();
    Assert.assertEquals(1, inflightLimiter.getMetrics().getAsyncQueueSize());
    futurePermit.cancel(false);
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCompletePermitDequeFuturePermit()
      throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = null;
    for (int i = 0; i < 2; ++i) {
      acquiredPermit = inflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = inflightLimiter.acquireAsync();
    Assert.assertEquals(1, inflightLimiter.getMetrics().getAsyncQueueSize());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed);
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
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
      inflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = inflightLimiter.acquireAsync();
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
      acquiredPermit = inflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = inflightLimiter.acquireAsync();
    Assert.assertFalse(futurePermit.isDone());
    futurePermit.completeExceptionally(new CancellationException()); // complete a future permit
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
    Assert.assertEquals(0, inflightLimiter.getMetrics().availablePermits());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed); // it should return permit
    Assert.assertEquals(0, inflightLimiter.getMetrics().getAsyncQueueSize());
    Assert.assertEquals(1, inflightLimiter.getMetrics().availablePermits());
  }
}
