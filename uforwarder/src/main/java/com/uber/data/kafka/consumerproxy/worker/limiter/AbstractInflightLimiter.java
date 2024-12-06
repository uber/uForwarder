package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

public abstract class AbstractInflightLimiter implements InflightLimiter {
  final Object lock = new Object();
  final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Override
  public Permit acquire(boolean dryRun) throws InterruptedException {
    Permit permit = doAcquire(dryRun ? AcquireMode.DryRun : AcquireMode.Blocking);
    if (permit == null) {
      // this should never happen
      return NoopPermit.INSTANCE;
    }
    return permit;
  }

  @Override
  public Optional<Permit> tryAcquire(boolean dryRun) {
    try {
      return Optional.ofNullable(doAcquire(dryRun ? AcquireMode.DryRun : AcquireMode.NonBlocking));
    } catch (InterruptedException e) {
      // this should never happen
      return Optional.empty();
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
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Acquires permit.
   *
   * @param acquireMode the acquire mode
   * @return the permit
   * @throws InterruptedException the interrupted exception
   */
  @Nullable
  abstract Permit doAcquire(AcquireMode acquireMode) throws InterruptedException;

  /** used by doAcquire() to determine how the request should be permitted */
  enum AcquireMode {
    // block the call until permit can be granted
    Blocking,
    // grant permit if possible, otherwise return NoopPermit.INSTANCE
    DryRun,
    // grant permit if possible, otherwise return null
    NonBlocking
  }

  abstract class Metrics implements InflightLimiter.Metrics {

    @Override
    public long availablePermits() {
      return getLimit() - getInflight();
    }
  }
}
