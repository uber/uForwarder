package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

public abstract class AbstractInflightLimiter implements InflightLimiter {
  final Object lock = new Object();
  final AtomicBoolean isClosed = new AtomicBoolean(false);
  final Queue<CompletableFuture<Permit>> futurePermits = new LinkedList<>();
  final AtomicInteger pendingPermitCount = new AtomicInteger();

  @Override
  public Permit acquire() throws InterruptedException {
    Permit permit = doAcquire(true);
    if (permit == null) {
      // this should never happen
      return NoopPermit.INSTANCE;
    }
    return permit;
  }

  @Override
  public Optional<Permit> tryAcquire() {
    try {
      return Optional.ofNullable(doAcquire(false));
    } catch (InterruptedException e) {
      // this should never happen
      return Optional.empty();
    }
  }

  /**
   * Acquire a permit asynchronously. if there is permit, return a complete future. otherwise future
   * enters a FIFO queue
   *
   * @return
   */
  @Override
  public CompletableFuture<Permit> acquireAsync() {
    Optional<Permit> permit = tryAcquire();
    if (permit.isPresent()) {
      return CompletableFuture.completedFuture(new AsyncPermit(permit.get()));
    } else {
      PermitCompletableFuture result = new PermitCompletableFuture();
      synchronized (AbstractInflightLimiter.this) {
        futurePermits.add(result);
      }
      return result;
    }
  }

  @Override
  public void close() {
    // do not change order of close
    // 1. set isClose to true
    // 2. unblock all threads waiting
    if (isClosed.compareAndSet(false, true)) {
      /** unblock all blocking threads */
      synchronized (lock) {
        lock.notifyAll();
      }
      /** complete all future permits */
      synchronized (this) {
        for (CompletableFuture<Permit> futurePermit : futurePermits) {
          futurePermit.complete(NoopPermit.INSTANCE);
        }
        futurePermits.clear();
      }
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Acquires permit.
   *
   * @param blocking blocks the call inorder to get permit
   * @return the permit
   * @throws InterruptedException the interrupted exception
   */
  @Nullable
  abstract Permit doAcquire(boolean blocking) throws InterruptedException;

  abstract class Metrics implements InflightLimiter.Metrics {
    @Override
    public long getAsyncQueueSize() {
      return pendingPermitCount.get();
    }

    @Override
    public long availablePermits() {
      return getLimit() - getInflight();
    }
  }

  private class AsyncPermit implements Permit {
    private final Permit nestedPermit;

    AsyncPermit(Permit permit) {
      nestedPermit = permit;
    }

    @Override
    public boolean complete(Result result) {
      boolean succeed = nestedPermit.complete(result);
      if (!succeed) {
        return false;
      }
      // try to complete any future permit if there is
      if (!futurePermits.isEmpty()) {
        Runnable runnable = null;
        synchronized (AbstractInflightLimiter.this) {
          if (!futurePermits.isEmpty()) {
            Optional<Permit> permit = tryAcquire();
            if (permit.isPresent()) {
              CompletableFuture<Permit> future;
              // purge completed futures and find one pending future on the head of queue
              while ((future = futurePermits.poll()) != null) {
                if (!future.isDone()) {
                  final CompletableFuture<Permit> finalFuture = future;
                  // reduce scope of critical zone by delay completion
                  runnable = () -> finalFuture.complete(new AsyncPermit(permit.get()));
                  break;
                }
              }
              if (future == null) {
                // complete the permit to avoid leaking
                permit.get().complete(Result.Unknown);
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

  private class PermitCompletableFuture extends CompletableFuture<Permit> {
    private PermitCompletableFuture() {
      pendingPermitCount.incrementAndGet();
    }

    @Override
    public boolean complete(Permit permit) {
      boolean completed = super.complete(permit);
      if (completed) {
        pendingPermitCount.decrementAndGet();
      } else {
        // avoid leaking of permit
        permit.complete(Result.Unknown);
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
      return complete(NoopPermit.INSTANCE);
    }
  }
}
