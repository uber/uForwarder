package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processor cancel result Messages could be slow processing, in case the messages needs to cancel
 * redirect processing to retry or stash
 */
class CancelResult {
  final ErrorCode errorCode;
  final DispatcherResponse.Code responseCode;
  private final AtomicBoolean closed;

  /**
   * Constructs a failed CancelResult
   *
   * @param errorCode indicates failed reason
   */
  CancelResult(ErrorCode errorCode) {
    this.errorCode = errorCode;
    this.responseCode = DispatcherResponse.Code.INVALID;
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Constructs a succeed CancelResult
   *
   * @param responseCode indicates responseCode
   */
  CancelResult(DispatcherResponse.Code responseCode) {
    this.errorCode = ErrorCode.INVALID;
    this.responseCode = responseCode;
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Close the result
   *
   * @param result indicates if cancel actually succeed
   * @return boolean indicates if close succeed
   */
  boolean close(boolean result) {
    if (closed.compareAndSet(false, true)) {
      return true;
    }
    return false;
  }

  boolean isSucceed() {
    return errorCode == ErrorCode.INVALID;
  }

  /** Cancel codes indicates error or fallback actions */
  enum ErrorCode {
    // Unexpected result
    INVALID,
    // throttled
    RATE_LIMITED,
    // blocking reason is not supported
    REASON_NOT_MATCH,
    // job configuration doesn't support cancel
    JOB_NOT_SUPPORTED
  }
}
