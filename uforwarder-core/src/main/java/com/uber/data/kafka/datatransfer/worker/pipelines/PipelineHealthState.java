package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/** The type Pipeline heath state. */
class PipelineHealthState {
  private final Ticker ticker;
  private final MutableHealthStateWindow[] windows;
  private final Duration windowDuration;
  private volatile int index;

  /**
   * Instantiates a new Pipeline heath state.
   *
   * @param ticker the ticker
   * @param windowDuration the window duration
   * @param windowCount the window count
   */
  PipelineHealthState(Ticker ticker, Duration windowDuration, int windowCount) {
    this.ticker = ticker;
    this.windows = new MutableHealthStateWindow[windowCount];
    this.windowDuration = windowDuration;
    this.index = 0;
    Arrays.fill(windows, new MutableHealthStateWindow());
  }

  /**
   * Record issue into the state
   *
   * @param issue the issue
   */
  void recordIssue(PipelineHealthIssue issue) {
    // TODO: implement the logic to record issue
  }

  /**
   * Gets state value. Value is a bit set of all issues
   *
   * @return the state value
   */
  int getStateValue() {
    int value = 0;
    for (MutableHealthStateWindow window : windows) {
      value |= window.getValue();
    }
    return value;
  }

  /** The type Mutable health state window. */
  @VisibleForTesting
  protected class MutableHealthStateWindow {
    private AtomicInteger value;

    protected MutableHealthStateWindow() {
      this.value = new AtomicInteger();
    }

    /**
     * Records issue into current window
     *
     * @param issue the issue
     */
    protected void recordIssue(PipelineHealthIssue issue) {
      while (true) {
        int oldValue = value.get();
        if (value.compareAndSet(oldValue, oldValue | issue.value())) {
          break;
        }
      }
    }

    /**
     * Gets state value The value is a bit set of the issues
     *
     * @return the value
     */
    int getValue() {
      return value.get();
    }
  }
}
