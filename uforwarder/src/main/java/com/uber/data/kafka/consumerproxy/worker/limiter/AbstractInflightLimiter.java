package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

public abstract class AbstractInflightLimiter implements InflightLimiter {
  final Object lock = new Object();
  final AtomicBoolean isClosed = new AtomicBoolean(false);
  final Set<CompletableFuture<Permit>> futurePermits = new LinkedHashSet<>();

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
      return new PermitCompletableFuture();
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
      return futurePermits.size();
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
              // complete the queued request on the head of queue
              Iterator<CompletableFuture<Permit>> iter = futurePermits.iterator();
              CompletableFuture<Permit> future = iter.next();
              iter.remove();
              // reduce scope of critical zone by delay completion
              runnable = () -> future.complete(new AsyncPermit(permit.get()));
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
      synchronized (AbstractInflightLimiter.this) {
        futurePermits.add(this);
      }
    }

    @Override
    public boolean complete(Permit permit) {
      beforeComplete();
      boolean result = super.complete(permit);
      if (!result) {
        // avoid leaking of permit
        permit.complete(Result.Unknown);
      }
      return result;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
      beforeComplete();
      return super.completeExceptionally(ex);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      // complete with noop permit
      return complete(NoopPermit.INSTANCE);
    }

    private void beforeComplete() {
      synchronized (AbstractInflightLimiter.this) {
        futurePermits.remove(this);
      }
    }
  }
}
