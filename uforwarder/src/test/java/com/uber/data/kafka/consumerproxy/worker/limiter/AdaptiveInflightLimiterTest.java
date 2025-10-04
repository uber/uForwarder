package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AdaptiveInflightLimiterTest {

  private AdaptiveInflightLimiter adaptiveInflightLimiter;
  VegasLimit vegasLimit = VegasLimit.newBuilder().initialLimit(2).build();
  SimpleLimiter simpleLimiter = SimpleLimiter.newBuilder().limit(vegasLimit).build();

  @BeforeEach
  public void setup() {
    adaptiveInflightLimiter =
        new AdaptiveInflightLimiter() {
          @Override
          public void setMaxInflight(int maxInflight) {
            vegasLimit =
                VegasLimit.newBuilder().initialLimit(2).maxConcurrency(maxInflight).build();
            simpleLimiter = SimpleLimiter.newBuilder().limit(vegasLimit).build();
          }

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
  }

  @Test
  public void testAcquire() throws InterruptedException {
    Assertions.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getLimit());
    adaptiveInflightLimiter.acquire();
    Assertions.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getLimit());
    adaptiveInflightLimiter.acquire();
    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());
    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getLimit());
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

    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());
    Awaitility.await()
        .atMost(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                Assertions.assertEquals(
                    1, adaptiveInflightLimiter.getMetrics().getBlockingQueueSize()));
  }

  @Test
  public void testRelease() throws InterruptedException {
    InflightLimiter.Permit permit = adaptiveInflightLimiter.acquire();
    Assertions.assertEquals(0, adaptiveInflightLimiter.getMetrics().getBlockingQueueSize());
    Assertions.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    permit.complete(InflightLimiter.Result.Succeed);
    Assertions.assertEquals(0, adaptiveInflightLimiter.getMetrics().getBlockingQueueSize());
    Assertions.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
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
      Assertions.assertTrue(false);
    }

    Assertions.assertTrue(reached.get());
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
      Assertions.assertTrue(false);
    }
  }

  @Test
  public void testAcquireWithRryRun() throws InterruptedException {
    adaptiveInflightLimiter.acquire(true);
    Assertions.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());

    adaptiveInflightLimiter.acquire(true);
    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());

    adaptiveInflightLimiter.acquire(true);
    Assertions.assertEquals(2, adaptiveInflightLimiter.getMetrics().getInflight());
  }

  @Test
  public void testTryAcquire() {
    Optional<InflightLimiter.Permit> permit1 = adaptiveInflightLimiter.tryAcquire();
    Assertions.assertTrue(permit1.isPresent());
    Optional<InflightLimiter.Permit> permit2 = adaptiveInflightLimiter.tryAcquire();
    Assertions.assertTrue(permit2.isPresent());
    Optional<InflightLimiter.Permit> permit3 = adaptiveInflightLimiter.tryAcquire();
    Assertions.assertFalse(permit3.isPresent());
    permit1.get().complete(InflightLimiter.Result.Succeed);
    permit3 = adaptiveInflightLimiter.tryAcquire();
    Assertions.assertTrue(permit3.isPresent());
  }
}
