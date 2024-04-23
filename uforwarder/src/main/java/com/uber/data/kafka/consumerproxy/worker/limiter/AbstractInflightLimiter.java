package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

public abstract class AbstractInflightLimiter implements InflightLimiter {
  final Object lock = new Object();
  final AtomicBoolean isClosed = new AtomicBoolean(false);

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
   * @param blocking blocks the call inorder to get permit
   * @return the permit
   * @throws InterruptedException the interrupted exception
   */
  @Nullable
  abstract Permit doAcquire(boolean blocking) throws InterruptedException;

  abstract class Metrics implements InflightLimiter.Metrics {

    @Override
    public long availablePermits() {
      return getLimit() - getInflight();
    }
  }
}
