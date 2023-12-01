package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.netflix.concurrency.limits.Limiter;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * AdaptiveInflightLimiter limits the number of infligt requests with floating limit. VegasLimit is
 * used to determine limit according to request latency and result
 *
 * <p>see {@link InflightLimiter}
 */
public abstract class AdaptiveInflightLimiter extends AbstractInflightLimiter {
  private final AtomicInteger queueLength;
  private final AtomicBoolean dryRun;

  public AdaptiveInflightLimiter() {
    this.dryRun = new AtomicBoolean(true);
    this.queueLength = new AtomicInteger(0);
  }

  /**
   * Sets dryRun mode. when dryRun is enabled, the limiter - permits all requests - evaluates
   * inflight limit
   *
   * @param dryRun the dry run
   */
  public void setDryRun(boolean dryRun) {
    this.dryRun.set(dryRun);
  }

  /**
   * Gets dryRun mode, when dryRun is enabled, the limiter - permits all requests
   *
   * @return
   */
  public boolean isDryRun() {
    return dryRun.get();
  }

  /**
   * internal implementation of tryAcquire
   *
   * @return the optional
   */
  abstract Optional<Limiter.Listener> tryAcquireImpl();

  @Override
  @Nullable
  Permit doAcquire(boolean blocking) throws InterruptedException {
    if (!isClosed.get()) {
      synchronized (lock) {
        queueLength.incrementAndGet();
        try {
          while (!isClosed.get()) {
            Optional<Limiter.Listener> listener = tryAcquireImpl();
            if (listener.isPresent()) {
              return new AdaptivePermit(listener.get());
            } else if (dryRun.get()) {
              return NoopPermit.INSTANCE;
            } else if (!blocking) {
              // unblock caller if failed to get permit
              return null;
            }

            // if not get permitted, wait for the unlock signal and try acquire permit again.
            lock.wait();
          }
        } finally {
          queueLength.decrementAndGet();
        }
      }
    }

    // Always permit if limiter is closed
    return NoopPermit.INSTANCE;
  }

  abstract class Metrics extends AbstractInflightLimiter.Metrics {
    @Override
    public long getBlockingQueueSize() {
      return queueLength.get();
    }
  }

  private class AdaptivePermit extends InflightLimiter.AbstractPermit {
    private Limiter.Listener listener;

    private AdaptivePermit(Limiter.Listener listener) {
      this.listener = listener;
    }

    @Override
    protected boolean doComplete(Result result) {
      switch (result) {
        case Failed:
          listener.onDropped();
          break;
        case Succeed:
          listener.onSuccess();
          break;
        default:
          listener.onIgnore();
      }
      /** unblock one blocking thread if there is any */
      synchronized (lock) {
        lock.notify();
      }
      return true;
    }
  }

  /** Builder builds AdaptiveInflightLimiter */
  public interface Builder {
    default Builder withLogEnabled(boolean logEnabled) {
      return this;
    }

    AdaptiveInflightLimiter build();
  }
}
