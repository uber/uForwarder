package com.uber.data.kafka.consumerproxy.worker.limiter;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
  Permit acquire() throws InterruptedException;

  /**
   * Acquires a permit from the limiter, when no enough permit, return null without block caller
   *
   * @return optional.Empty if failed to get permit
   */
  Optional<Permit> tryAcquire();

  /**
   * Acquires a permit asynchronously
   *
   * @return
   */
  CompletableFuture<Permit> acquireAsync();

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
    /** indicates request failed */
    Failed,
    /** indicates result is unknown */
    Unknown
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
     * Gets number of async requests in the queue
     *
     * @return
     */
    long getAsyncQueueSize();

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
     * complete request with unknown result
     *
     * @return the boolean
     */
    default boolean complete() {
      return complete(Result.Unknown);
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
