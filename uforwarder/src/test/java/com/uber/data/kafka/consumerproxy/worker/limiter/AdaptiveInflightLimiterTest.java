package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
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

public class AdaptiveInflightLimiterTest extends FievelTestBase {

  private AdaptiveInflightLimiter adaptiveInflightLimiter;

  @Before
  public void setup() {
    VegasLimit vegasLimit = VegasLimit.newBuilder().initialLimit(2).build();
    SimpleLimiter simpleLimiter = SimpleLimiter.newBuilder().limit(vegasLimit).build();
    adaptiveInflightLimiter =
        new AdaptiveInflightLimiter() {
          @Override
          Optional<Limiter.Listener> tryAcquireImpl() {
            return simpleLimiter.acquire(null);
          }

          @Override
          public Metrics getMetrics() {
            return new AdaptiveInflightLimiter.Metrics() {
              @Override
              public long getInflight() {
                return simpleLimiter.getInflight();
              }

              @Override
              public long getLimit() {
                return simpleLimiter.getLimit();
              }
            };
          }
        };

    adaptiveInflightLimiter.setDryRun(false);
  }

  @Test
  public void testAcquire() throws InterruptedException {
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getLimit());
    adaptiveInflightLimiter.acquire();
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getLimit());
    adaptiveInflightLimiter.acquire();
    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());
    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getLimit());
  }

  @Test
  public void testAcquireMore() throws InterruptedException {
    adaptiveInflightLimiter.acquire();
    adaptiveInflightLimiter.acquire();
    CompletableFuture.runAsync(
        () -> {
          try {
            adaptiveInflightLimiter.acquire();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());
    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assert.assertEquals(
                    1, adaptiveInflightLimiter.getMetrics().getBlockingQueueSize()));
  }

  @Test
  public void testRelease() throws InterruptedException {
    InflightLimiter.Permit permit = adaptiveInflightLimiter.acquire();
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    permit.complete(InflightLimiter.Result.Succeed);
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getBlockingQueueSize());
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
  }

  @Test
  public void testAcquireAndRelease() throws InterruptedException {
    AtomicBoolean reached = new AtomicBoolean(false);
    adaptiveInflightLimiter.acquire();
    InflightLimiter.Permit permit = adaptiveInflightLimiter.acquire();
    Thread thread =
        new Thread(
            () -> {
              try {
                adaptiveInflightLimiter.acquire();
                reached.set(true);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    thread.start();

    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> adaptiveInflightLimiter.getMetrics().getBlockingQueueSize() == 1);
    permit.complete(InflightLimiter.Result.Succeed);

    try {
      thread.join(1000);
    } catch (InterruptedException e) {
      Assert.assertTrue(false);
    }

    Assert.assertTrue(reached.get());
  }

  @Test
  public void testAcquireAndInterrupt() throws InterruptedException {
    adaptiveInflightLimiter.acquire();
    adaptiveInflightLimiter.acquire();
    Thread thread =
        new Thread(
            () -> {
              try {
                adaptiveInflightLimiter.acquire();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    thread.start();
    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> adaptiveInflightLimiter.getMetrics().getBlockingQueueSize() == 1);

    thread.interrupt();

    try {
      thread.join(100);
    } catch (InterruptedException e) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testAcquireWithRryRun() throws InterruptedException {
    adaptiveInflightLimiter.setDryRun(true);

    adaptiveInflightLimiter.acquire();
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());

    adaptiveInflightLimiter.acquire();
    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());

    adaptiveInflightLimiter.acquire();
    Assert.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());
  }

  @Test
  public void testTryAcquire() {
    Optional<InflightLimiter.Permit> permit1 = adaptiveInflightLimiter.tryAcquire();
    Assert.assertTrue(permit1.isPresent());
    Optional<InflightLimiter.Permit> permit2 = adaptiveInflightLimiter.tryAcquire();
    Assert.assertTrue(permit2.isPresent());
    Optional<InflightLimiter.Permit> permit3 = adaptiveInflightLimiter.tryAcquire();
    Assert.assertFalse(permit3.isPresent());
    permit1.get().complete(InflightLimiter.Result.Succeed);
    permit3 = adaptiveInflightLimiter.tryAcquire();
    Assert.assertTrue(permit3.isPresent());
  }

  @Test
  public void testIsDryRun() {
    Assert.assertFalse(false);
    adaptiveInflightLimiter.setDryRun(true);
    Assert.assertTrue(adaptiveInflightLimiter.isDryRun());
  }

  @Test
  public void testAcquireAsync() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> futurePermit1 =
        adaptiveInflightLimiter.acquireAsync();
    Assert.assertTrue(futurePermit1.isDone());
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit2 =
        adaptiveInflightLimiter.acquireAsync();
    Assert.assertTrue(futurePermit2.isDone());
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
    CompletableFuture<InflightLimiter.Permit> futurePermit3 =
        adaptiveInflightLimiter.acquireAsync();
    Assert.assertFalse(futurePermit3.isDone());
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
    Assert.assertTrue(futurePermit2.get().complete(InflightLimiter.Result.Succeed));
    Assert.assertTrue(futurePermit3.isDone());
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCloseRelease() {
    List<CompletableFuture<InflightLimiter.Permit>> permits = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      permits.add(adaptiveInflightLimiter.acquireAsync());
    }
    int done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assert.assertEquals(2, done);
    adaptiveInflightLimiter.close();
    done = 0;
    for (CompletableFuture<InflightLimiter.Permit> permit : permits) {
      done += permit.isDone() ? 1 : 0;
    }
    Assert.assertEquals(3, done);
  }

  @Test
  public void testAcquireSucceedAndCancel() {
    CompletableFuture<InflightLimiter.Permit> futurePermit = adaptiveInflightLimiter.acquireAsync();
    Assert.assertFalse(futurePermit.cancel(false));
  }

  @Test
  public void testAcquireFailedAndCancel() {
    for (int i = 0; i < 2; ++i) {
      adaptiveInflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adaptiveInflightLimiter.acquireAsync();
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
    futurePermit.cancel(false);
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCompletePermitDequeFuturePermit()
      throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> acquiredPermit = null;
    for (int i = 0; i < 2; ++i) {
      acquiredPermit = adaptiveInflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adaptiveInflightLimiter.acquireAsync();
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
    acquiredPermit.get().complete(InflightLimiter.Result.Succeed);
    Assert.assertTrue(futurePermit.isDone());
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getAsyncQueueSize());
  }

  @Test
  public void testCompletePermitCompletableFuture() {
    for (int i = 0; i < 2; ++i) {
      adaptiveInflightLimiter.acquireAsync();
    }
    CompletableFuture<InflightLimiter.Permit> futurePermit = adaptiveInflightLimiter.acquireAsync();
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
}
