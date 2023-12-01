package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * A lease tracks the validity of a resource that is acquired.
 *
 * <p>The lifecycle of a lease is:
 *
 * <ul>
 *   <li>The lease is created. Internally, the lease marks the current timestamp.
 *   <li>The owner of the lease may extend a lease by calling {@code success}. This will update the
 *       last success time of the lease
 *   <li>The owner of the lease can check the validity of the lease by calling {@code isValid} and
 *       perform any follow up action as necessary
 * </ul>
 */
@InternalApi
final class Lease {
  private final long leaseDurationMs;
  private LongAccumulator lastSuccess;

  Lease(long leaseDurationMs) {
    this(leaseDurationMs, System.currentTimeMillis());
  }

  private Lease(long leaseDurationMs, long lastSuccess) {
    this.leaseDurationMs = leaseDurationMs;
    this.lastSuccess = new LongAccumulator(Long::max, lastSuccess);
  }

  /** Create a new lease with mocked lastSuccess time for test. */
  @VisibleForTesting
  static Lease forTest(long leaseDurationMs, long lastSuccess) {
    return new Lease(leaseDurationMs, lastSuccess);
  }

  /** update the lastSuccess timestamp for this lease. */
  Lease success() {
    return success(System.currentTimeMillis());
  }

  private Lease success(final long currentTime) {
    lastSuccess.accumulate(currentTime);
    return this;
  }

  /**
   * isValid checks whether the current lease is valid.
   *
   * @return true if the lease is valid, otherwise false.
   */
  boolean isValid() {
    return isValid(System.currentTimeMillis());
  }

  private boolean isValid(final long currentTime) {
    return lastSuccess.get() + leaseDurationMs > currentTime;
  }

  /**
   * Return the last success time.
   *
   * @return epoch time of last success
   */
  @VisibleForTesting
  long lastSuccessTime() {
    return lastSuccess.get();
  }
}
