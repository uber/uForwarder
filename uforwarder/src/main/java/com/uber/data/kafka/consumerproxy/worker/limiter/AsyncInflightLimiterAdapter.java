package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Supports acquire permit asynchronously use nested {@link InflightLimiter} as delegator Note,
 * adapter needs to be closed independently from deleagtor
 */
public class AsyncInflightLimiterAdapter implements AutoCloseable {
  private final Queue<CompletableFuture<InflightLimiter.Permit>> futurePermits = new LinkedList<>();
  private final AtomicInteger pendingPermitCount = new AtomicInteger();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final InflightLimiter delegator;
  private final Metrics metrics;

  private AsyncInflightLimiterAdapter(InflightLimiter delegator) {
    this.delegator = delegator;
    this.metrics = new Metrics();
  }

  /**
   * Acquires a permit asynchronously. if there is permit, return a complete future. otherwise
   * future enters a FIFO queue
   *
   * @return
   */
  public CompletableFuture<InflightLimiter.Permit> acquireAsync() {
    Optional<InflightLimiter.Permit> permit = delegator.tryAcquire();
    if (permit.isPresent()) {
      return CompletableFuture.completedFuture(new AsyncPermit(permit.get()));
    } else {
      PermitCompletableFuture result = new PermitCompletableFuture();
      synchronized (this) {
        futurePermits.add(result);
      }
      return result;
    }
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      /** complete all future permits */
      synchronized (this) {
        for (CompletableFuture<InflightLimiter.Permit> futurePermit : futurePermits) {
          futurePermit.complete(InflightLimiter.NoopPermit.INSTANCE);
        }
        futurePermits.clear();
      }
    }
  }

  /**
   * Gets metrics the adapter
   *
   * @return
   */
  public Metrics getMetrics() {
    return metrics;
  }

  /**
   * Creates new instance of adapter
   *
   * @param delegator
   * @return
   */
  public static AsyncInflightLimiterAdapter of(InflightLimiter delegator) {
    return new AsyncInflightLimiterAdapter(delegator);
  }

  private class AsyncPermit implements InflightLimiter.Permit {
    private final InflightLimiter.Permit nestedPermit;

    AsyncPermit(InflightLimiter.Permit permit) {
      nestedPermit = permit;
    }

    @Override
    public boolean complete(InflightLimiter.Result result) {
      boolean succeed = nestedPermit.complete(result);
      if (!succeed) {
        return false;
      }
      // try to complete any future permit if there is
      if (!futurePermits.isEmpty()) {
        Runnable runnable = null;
        synchronized (AsyncInflightLimiterAdapter.this) {
          if (!futurePermits.isEmpty()) {
            Optional<InflightLimiter.Permit> permit = delegator.tryAcquire();
            if (permit.isPresent()) {
              CompletableFuture<InflightLimiter.Permit> future;
              // purge completed futures and find one pending future on the head of queue
              while ((future = futurePermits.poll()) != null) {
                if (!future.isDone()) {
                  final CompletableFuture<InflightLimiter.Permit> finalFuture = future;
                  // reduce scope of critical zone by delay completion
                  runnable = () -> finalFuture.complete(new AsyncPermit(permit.get()));
                  break;
                }
              }
              if (future == null) {
                // complete the permit to avoid leaking
                permit.get().complete(InflightLimiter.Result.Unknown);
              }
            }
          }
        }
        if (runnable != null) {
          runnable.run();
        }
      }
      return true;
    }
  }

  private class PermitCompletableFuture extends CompletableFuture<InflightLimiter.Permit> {
    private PermitCompletableFuture() {
      pendingPermitCount.incrementAndGet();
    }

    @Override
    public boolean complete(InflightLimiter.Permit permit) {
      boolean completed = super.complete(permit);
      if (completed) {
        pendingPermitCount.decrementAndGet();
      } else {
        // avoid leaking of permit
        permit.complete(InflightLimiter.Result.Unknown);
      }
      return completed;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
      boolean completed = super.completeExceptionally(ex);
      if (completed) {
        pendingPermitCount.decrementAndGet();
      }
      return completed;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      // complete with noop permit
      return complete(InflightLimiter.NoopPermit.INSTANCE);
    }
  }

  /** Adapter metrics */
  public class Metrics {

    public long getAsyncQueueSize() {
      return pendingPermitCount.get();
    }
  }
}
