package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * As a part of flow control functionality InflightLimiter provides capability to limit the number
 * of inflight requests sending to a down stream service.
 *
 * <p>usages, a client need to acquire() a permit first, do some work then release the permit. e.g.
 *
 * <pre>
 *   Permit permit = limiter.acquire();
 *   //do some work
 *   //permit.complete(Result.Succeed)
 * </pre>
 */
public interface InflightLimiter extends AutoCloseable {
  /**
   * Acquires a permit from the limiter The method may be blocked if there is no enough permit
   *
   * @return the permit
   * @throws InterruptedException the interrupted exception
   */
  default Permit acquire() throws InterruptedException {
    return acquire(false);
  }

  /**
   * Acquires a permit from the limiter The method may be blocked if there is no enough permit
   *
   * @param dryRun return NoopPermit without blocking caller
   * @return the permit
   * @throws InterruptedException the interrupted exception
   */
  Permit acquire(boolean dryRun) throws InterruptedException;

  /**
   * Acquires a permit from the limiter, when no enough permit, return result without block caller
   *
   * @return optional.Empty if failed to get permit
   */
  default Optional<Permit> tryAcquire() {
    return tryAcquire(false);
  }

  /**
   * Acquires a permit from the limiter, when no enough permit, return result without block caller
   *
   * @param dryRun return NoopPermit without return empty permit
   * @return optional.Empty if failed to get permit
   */
  Optional<Permit> tryAcquire(boolean dryRun);

  /**
   * Gets if the limiter is closed
   *
   * @return the boolean
   */
  boolean isClosed();

  /**
   * Gets metrics of the limiter
   *
   * @return the metrics
   */
  Metrics getMetrics();

  /** Request result feedback to the limiter */
  enum Result {
    /** indicates request succeed */
    Succeed,
    /** indicates request unknown failure */
    Failed,
    /** indicates request dropped */
    Dropped
  }

  /** Metrics of inflight limiter */
  interface Metrics {

    /**
     * Gets the number of inflight requests
     *
     * @return the inflight
     */
    long getInflight();

    /**
     * Gets the inflight limit.
     *
     * @return the limit
     */
    long getLimit();

    /**
     * Gets available permit count
     *
     * @return
     */
    long availablePermits();

    /**
     * Gets the number of blocked requests
     *
     * @return the blocked requests length;
     */
    long getBlockingQueueSize();

    /**
     * Gets extra metrics
     *
     * @return the extra status
     */
    default Map<String, Double> getExtraMetrics() {
      return Collections.emptyMap();
    }
  }

  /** Permit is granted by the limiter. A permit can be released by calling complete() function */
  interface Permit {

    /**
     * complete request and release permit
     *
     * @param result indicates request result
     * @return the boolean indicates success/failure of completion
     */
    boolean complete(Result result);

    /**
     * complete request successfully
     *
     * @return the boolean
     */
    default boolean complete() {
      return complete(Result.Succeed);
    }
  }

  abstract class AbstractPermit implements Permit {
    private final AtomicBoolean completed = new AtomicBoolean(false);

    protected abstract boolean doComplete(Result result);

    @Override
    public boolean complete(Result result) {
      if (completed.compareAndSet(false, true)) {
        return doComplete(result);
      }
      return false;
    }
  }

  class NoopPermit implements InflightLimiter.Permit {

    /** The Instance. */
    public static NoopPermit INSTANCE = new NoopPermit();

    @Override
    public boolean complete(Result result) {
      return true;
    }
  }
}
