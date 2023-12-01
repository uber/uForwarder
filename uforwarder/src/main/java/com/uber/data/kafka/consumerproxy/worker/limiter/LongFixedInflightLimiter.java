package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * LongFixedInflightLimiter limits the number of infligt requests with fixed limit.
 *
 * <p>see {@link InflightLimiter}
 */
public class LongFixedInflightLimiter extends AbstractInflightLimiter {
  private final AtomicLong queueLength;
  private final AtomicLong inflight;
  private final InflightLimiter.Metrics metrics;
  private volatile long limit;

  public LongFixedInflightLimiter(long limit) {
    this.limit = Math.max(0, limit);
    this.queueLength = new AtomicLong(0);
    this.inflight = new AtomicLong(0);
    this.metrics = new FixedMetrics();
  }

  /**
   * Acquires specific number of permits from the limiter The method may be blocked if there is no
   * enough permit
   *
   * @param n the number of permits to get
   * @return the permit
   * @throws InterruptedException the interrupted exception
   */
  public Permit acquire(int n) throws InterruptedException {
    Permit permit = doAcquire(true, n);
    if (permit == null) {
      // this should never happen
      return NoopPermit.INSTANCE;
    }
    return permit;
  }

  /**
   * Acquires specific number of permits from the limiter, when no enough permit, return null
   * without block caller
   *
   * @param n the number of permits to get
   * @return optional.Empty if failed to get permit
   */
  public Optional<Permit> tryAcquire(int n) {
    try {
      return Optional.ofNullable(doAcquire(false, n));
    } catch (InterruptedException e) {
      // this should never happen
      return Optional.empty();
    }
  }

  @Override
  public InflightLimiter.Metrics getMetrics() {
    return metrics;
  }

  /**
   * Update inflight limit to another fixed value.
   *
   * @param limit the limit
   */
  public synchronized void updateLimit(long limit) {
    limit = Math.max(0, limit);
    long oldLimit = this.limit;
    this.limit = limit;
    if (oldLimit == limit) {
      return;
    } else if (oldLimit < limit) {
      /** unblock one blocking thread if there is any */
      synchronized (lock) {
        lock.notify();
      }
    } else if (limit == 0) {
      /** unblock all blocking threads if there is any */
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  @Nullable
  @Override
  Permit doAcquire(boolean blocking) throws InterruptedException {
    return doAcquire(blocking, 1);
  }

  @Nullable
  Permit doAcquire(boolean blocking, int n) throws InterruptedException {
    if (n < 0) {
      return NoopPermit.INSTANCE;
    }

    if (!isClosed.get() && limit != 0) {
      synchronized (lock) {
        queueLength.addAndGet(n);
        try {
          while (!isClosed.get() && limit != 0) {
            long nInflight = inflight.get();
            if (nInflight >= 0
                && // note, nInflight could overflow
                nInflight <= limit - n) {
              inflight.addAndGet(n);
              return new FixedPermit(n);
            } else if (!blocking) {
              // unblock caller if failed to get permit
              return null;
            }
            // if not get permitted, wait for the unlock signal and try acquire permit again.
            lock.wait();
          }
        } finally {
          queueLength.addAndGet(-n);
        }
      }
    }

    // Always permit if limiter is closed
    return NoopPermit.INSTANCE;
  }

  private class FixedPermit extends InflightLimiter.AbstractPermit {
    private final long amount;

    private FixedPermit(long amount) {
      this.amount = amount;
    }

    @Override
    protected boolean doComplete(Result result) {
      inflight.addAndGet(-amount);
      /** unblock one blocking thread if there is any */
      synchronized (lock) {
        lock.notify();
      }
      return true;
    }
  }

  private class FixedMetrics extends AbstractInflightLimiter.Metrics {

    @Override
    public long getInflight() {
      return inflight.get();
    }

    @Override
    public long getLimit() {
      return limit;
    }

    @Override
    public long getBlockingQueueSize() {
      return queueLength.get();
    }
  }
}
